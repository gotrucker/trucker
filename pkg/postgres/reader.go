package postgres

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"text/template"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type Reader struct {
	queryTemplate *template.Template
	conn          *pgxpool.Pool
}

func NewReader(readQuery string, cfg config.Connection) *Reader {
	tmpl, err := template.New("inputSql").Parse(readQuery)
	if err != nil {
		panic(err)
	}

	conn := NewConnection(cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database, false)

	return &Reader{queryTemplate: tmpl, conn: conn}
}

func (r *Reader) Read(changeset *db.Changeset) *db.Changeset {
	var rows pgx.Rows

	// We need to hold on to a specific connection to be able to create and
	// access the temporary table until we're done (in case we're not using a
	// VALUES list)
	conn, err := r.conn.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	defer conn.Release()

	if len(changeset.Columns) == 0 || len(changeset.Values) == 0 {
		return nil
	} else if len(changeset.Columns)*len(changeset.Values) > maxPreparedStatementArgs {
		// Use a temporary table instead of a VALUES list in case we go over the
		// maximum number of prepared statement arguments
		rows = r.readUsingTempTable(conn, changeset)
	} else {
		rows = r.readUsingValuesList(changeset)
	}

	defer rows.Close()

	cols := make([]db.Column, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		cols[i] = db.Column{
			Name: field.Name,
			Type: oidToDbType(field.DataTypeOID),
		}
	}

	vals := make([][]any, 0, 1)
	for i := 0; rows.Next(); i++ {
		rowVals, err := rows.Values()
		if err != nil {
			panic(err)
		}
		vals = append(vals, rowVals)
	}

	return &db.Changeset{
		Operation: changeset.Operation,
		Table:     changeset.Table,
		Columns:   cols,
		Values:    vals,
	}
}

func (r *Reader) Close() {
	r.conn.Close()
}

// - transform the input args into a postgres values literal
// - feed that to the template as a .rows variable
// - run the query with values args and return the result
func (r *Reader) readUsingValuesList(changeset *db.Changeset) pgx.Rows {
	valuesLiteral, values := makeValuesLiteral(changeset.Columns, changeset.Values)

	tmplVars := map[string]string{
		"operation": db.OperationStr(changeset.Operation),
		"rows":      valuesLiteral.String(),
	}
	sql := new(bytes.Buffer)
	err := r.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	rows, err := r.conn.Query(context.Background(), sql.String(), values...)
	if err != nil {
		log.Printf("[Postgres Reader] Error running query:\n%s\n", sql.String())
		log.Printf("[Postgres Reader] Query values:\n%v\n", values)
		panic(err)
	}

	return rows
}

func (r *Reader) readUsingTempTable(conn *pgxpool.Conn, changeset *db.Changeset) pgx.Rows {
	// Create a temporary table to store the rows
	_, err := conn.Query(context.Background(), "CREATE TEMPORARY TABLE r "+changeset.Table)
	if err != nil {
		panic(err)
	}

	columns := make([]string, len(changeset.Columns))
	for i, col := range changeset.Columns {
		columns[i] = col.Name
	}
	baseSql := fmt.Sprintf("INSERT INTO r (%s) VALUES ", strings.Join(columns, ","))
	chunkSize := maxPreparedStatementArgs / len(changeset.Columns)

	// TODO: We don't need to rebuild the string over and over again. We can reuse it for all of the chunks except the last one if that one's smaller.
	for chunk := range slices.Chunk(changeset.Values, chunkSize) {
		sb := strings.Builder{}
		sb.WriteString(baseSql)

		for i := range len(chunk) {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')

			for j := range len(columns) {
				if j > 0 {
					sb.WriteByte(',')
				}

				sb.WriteString(fmt.Sprintf(
					"$%d", // "$%d::%s",
					(i*len(columns))+j+1,
					// dbTypeToPgType(changeset.Columns[j].Type)
				))
			}

			sb.WriteByte(')')
		}

		_, err = conn.Query(context.Background(), sb.String(), chunk)
		if err != nil {
			panic(err)
		}
	}

	// TODO: The code from here on is mostly the same as readUsingValuesLiteral. We should refactor this to avoid code duplication.
	tmplVars := map[string]string{
		"operation": db.OperationStr(changeset.Operation),
		"rows":      "r",
	}
	sql := new(bytes.Buffer)
	err = r.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	rows, err := r.conn.Query(context.Background(), sql.String())
	if err != nil {
		log.Printf("[Postgres Reader] Error running query:\n%s\n", sql.String())
		panic(err)
	}
	return rows
}
