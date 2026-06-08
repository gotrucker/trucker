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
	"github.com/tonyfg/trucker/pkg/metrics"
)

type Reader struct {
	truckName     string
	queryTemplate *template.Template
	conn          *pgxpool.Pool
}

func NewReader(truckName string, readQuery string, cfg config.Connection) *Reader {
	tmpl, err := template.New("inputSql").Parse(readQuery)
	if err != nil {
		log.Println("Error parsing input SQL template:\n", readQuery)
		metrics.DBErrors.WithLabelValues("postgres", "reader").Inc()
		panic(err)
	}

	conn := NewConnection(cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database, cfg.Ssl, false)

	return &Reader{truckName: truckName, queryTemplate: tmpl, conn: conn}
}

func (r *Reader) Read(changes *db.Changes) *db.Changes {
	rows := <-changes.Rows
	if len(changes.Columns) == 0 || len(rows) == 0 {
		return nil
	}

	for rowBatch := range changes.Rows {
		rows = append(rows, rowBatch...)

		if len(changes.Columns)*len(rows) > maxPreparedStatementArgs {
			break
		}
	}

	var flatValues []any
	columnsLiteral := makeColumnsList(changes.Columns).String()
	tmplVars := map[string]string{
		"operation":   db.OperationStr(changes.Operation),
		"input_table": changes.Table,
	}

	// We need to hold on to a specific connection to be able to create and
	// access the temporary table until we're done (in case we're not using a
	// VALUES list)
	conn, err := r.conn.Acquire(context.Background())
	if err != nil {
		panic(err)
	}

	if len(changes.Columns)*len(rows) <= maxPreparedStatementArgs {
		// All of the data fits in a single query using a VALUES list. Let's do it!
		valuesList, values := makeValuesList(changes.Columns, rows, true)
		flatValues = values
		sb := strings.Builder{}
		sb.WriteString("(VALUES ")
		sb.WriteString(valuesList.String())
		sb.WriteString(") AS r (")
		sb.WriteString(columnsLiteral)
		sb.WriteByte(')')
		tmplVars["rows"] = sb.String()
		metrics.QueryMode.WithLabelValues(r.truckName, "reader", "values", "postgres").Inc()
	} else {
		// Load in batches to a temporary table instead of using a VALUES list
		// since we're over the maximum number of parameters supported by PG for
		// a SQL query.
		log.Printf(
			"[Postgres Reader] Reading changeset with more than 32k parameters for %s on table %s. Using temporary table...\n",
			db.OperationStr(changes.Operation),
			changes.Table,
		)
		tmplVars["rows"] = "r"
		r.prepareTempTable(conn, changes, columnsLiteral, rows)
		metrics.QueryMode.WithLabelValues(r.truckName, "reader", "temp_table", "postgres").Inc()
	}

	sql := new(bytes.Buffer)
	err = r.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	results, err := conn.Query(context.Background(), sql.String(), flatValues...)
	if err != nil {
		log.Printf("[Postgres Reader] Error running query:\n%s\n", sql.String())
		log.Printf("[Postgres Reader] Query values:\n%v\n", flatValues)
		metrics.DBErrors.WithLabelValues("postgres", "reader").Inc()
		panic(err)
	}

	fields := results.FieldDescriptions()
	cols := make([]db.Column, len(fields))
	for i, field := range fields {
		cols[i] = db.Column{
			Name: field.Name,
			Type: oidToDbType(field.DataTypeOID),
		}
	}

	rowChan := make(chan [][]any, channelSize)

	truckName := r.truckName
	table := changes.Table
	op := db.OperationStr(changes.Operation)

	// TODO This go routine is basically the same between reader and backfill. Refactor to avoid dups
	go func() {
		defer conn.Release()
		defer conn.Exec(context.Background(), "DROP TABLE IF EXISTS r")
		defer results.Close()
		defer close(rowChan)

		rowBatch := make([][]any, 0, batchSize)
		var rowCount int64

		for results.Next() {
			row, err := results.Values()
			if err != nil {
				metrics.DBErrors.WithLabelValues("postgres", "reader").Inc()
				panic(err)
			}

			rowBatch = append(rowBatch, row)
			rowCount++

			if len(rowBatch) == batchSize {
				rowChan <- rowBatch
				rowBatch = make([][]any, 0, batchSize)
			}
		}

		if len(rowBatch) > 0 {
			rowChan <- rowBatch
		}

		if rowCount > 0 {
			metrics.RowsRead.WithLabelValues(truckName, table, op).Add(float64(rowCount))
		}
	}()

	return &db.Changes{
		Operation: changes.Operation,
		Table:     changes.Table,
		Columns:   cols,
		Rows:      rowChan,
	}
}

func (r *Reader) Close() {
	r.conn.Close()
}

func (r *Reader) prepareTempTable(conn *pgxpool.Conn, changes *db.Changes, columnsLiteral string, rows [][]any) {
	// Create a temporary table to store the rows
	sb := strings.Builder{}
	sb.WriteString("CREATE TEMPORARY TABLE r (")
	for i, col := range changes.Columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("%s %s", col.Name, dbTypeToPgType(col.Type)))
	}
	sb.WriteByte(')')

	_, err := conn.Exec(context.Background(), sb.String())
	if err != nil {
		log.Printf("[Postgres Reader] Error executing SQL:\n%s", sb.String())
		metrics.DBErrors.WithLabelValues("postgres", "reader").Inc()
		panic(err)
	}

	baseSql := fmt.Sprintf("INSERT INTO r (%s) VALUES ", columnsLiteral)
	numCols := len(changes.Columns)
	chunkSize := maxPreparedStatementArgs / numCols

	for chunk := range slices.Chunk(rows, chunkSize) {
		insertToTempTable(conn, baseSql, changes.Columns, chunk)
	}
	for chunk := range changes.Rows {
		insertToTempTable(conn, baseSql, changes.Columns, chunk)
	}
}

// TODO [PERFORMANCE] We don't need to rebuild the string over and over again every time this is called. We can reuse it for all of the chunks except the last one if that one's smaller.
func insertToTempTable(conn *pgxpool.Conn, baseSql string, columns []db.Column, rows [][]any) {
	sb := strings.Builder{}
	sb.WriteString(baseSql)
	valuesList, flatValues := makeValuesList(columns, rows, false)
	sb.WriteString(valuesList.String())

	_, err := conn.Exec(context.Background(), sb.String(), flatValues...)
	if err != nil {
		metrics.DBErrors.WithLabelValues("postgres", "reader").Inc()
		panic(err)
	}
}
