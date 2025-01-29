package postgres

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"text/template"

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

func (r *Reader) Read(changeset *db.Changeset) *db.ChanChangeset {
	// We need to hold on to a specific connection to be able to create and
	// access the temporary table until we're done (in case we're not using a
	// VALUES list)
	conn, err := r.conn.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	defer conn.Release()

	var flatValues []any
	columnsLiteral := makeColumnsList(changeset.Columns).String()
	tmplVars := map[string]string{"operation": db.OperationStr(changeset.Operation)}

	if len(changeset.Columns)*len(changeset.Rows) <= maxPreparedStatementArgs {
		// All of the data fits in a single query using a VALUES list. Let's do it!
		valuesList, values := makeValuesList(changeset.Columns, changeset.Rows)
		flatValues = values
		sb := strings.Builder{}
		sb.WriteString("(VALUES ")
		sb.WriteString(valuesList.String())
		sb.WriteString(") AS r (")
		sb.WriteString(columnsLiteral)
		sb.WriteByte(')')
		tmplVars["rows"] = sb.String()
	} else {
		// Load in batches to a temporary table instead of using a VALUES list
		// since we're over the maximum number of parameters supported by PG for
		// a SQL query.
		log.Printf("[Postgres Reader] Insert with more than 32k parameters. Using temporary table for %d rows\n", len(changeset.Rows))
		tmplVars["rows"] = "r"
		r.prepareTempTable(conn, changeset, columnsLiteral, changeset.Rows)
	}

	sql := new(bytes.Buffer)
	err = r.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	rows, err := r.conn.Query(context.Background(), sql.String(), flatValues...)
	if err != nil {
		log.Printf("[Postgres Reader] Error running query:\n%s\n", sql.String())
		log.Printf("[Postgres Reader] Query values:\n%v\n", flatValues)
		panic(err)
	}

	fields := rows.FieldDescriptions()
	cols := make([]db.Column, len(fields))
	for i, field := range fields {
		cols[i] = db.Column{
			Name: field.Name,
			Type: oidToDbType(field.DataTypeOID),
		}
	}

	rowChan := make(chan [][]any, channelSize)

	// TODO This go routine is basically the same between reader and backfill. Refactor to avoid dups
	go func() {
		defer rows.Close()
		defer close(rowChan)

		rowBatch := make([][]any, 0, batchSize)

		for rows.Next() {
			row, err := rows.Values()
			if err != nil {
				panic(err)
			}

			rowBatch = append(rowBatch, row)

			if len(rowBatch) == batchSize {
				rowChan <- rowBatch
				rowBatch = make([][]any, 0, batchSize)
			}

		}

		if len(rowBatch) > 0 {
			rowChan <- rowBatch
		}
	}()

	return &db.ChanChangeset{
		Operation: changeset.Operation,
		Table:     changeset.Table,
		Columns:   cols,
		Rows:      rowChan,
	}
}

func (r *Reader) Close() {
	r.conn.Close()
}

func (r *Reader) prepareTempTable(conn *pgxpool.Conn, changeset *db.Changeset, columnsLiteral string, rows [][]any) {
	// Create a temporary table to store the rows
	_, err := conn.Exec(
		context.Background(),
		fmt.Sprintf("CREATE TEMPORARY TABLE r (LIKE %s)", changeset.Table),
	)
	if err != nil {
		panic(err)
	}

	baseSql := fmt.Sprintf("INSERT INTO r (%s) VALUES ", columnsLiteral)
	numCols := len(changeset.Columns)
	chunkSize := maxPreparedStatementArgs / numCols

	// TODO [PERFORMANCE] We don't need to rebuild the string over and over again. We can reuse it for all of the chunks except the last one if that one's smaller.
	for chunk := range slices.Chunk(rows, chunkSize) {
		sb := strings.Builder{}
		sb.WriteString(baseSql)

		for i := range len(chunk) {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')

			for j := range numCols {
				if j > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("$%d", (i*numCols)+j+1))
			}

			sb.WriteByte(')')
		}

		_, err = conn.Exec(context.Background(), sb.String(), chunk)
		if err != nil {
			panic(err)
		}
	}
}
