package postgres

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/tonyfg/trucker/pkg/db"
)

const backfillBatchSize = 50000

type BackfillBatch struct {
	Columns []string
	Types   []string
	Rows    [][]any
}

func (rc *ReplicationClient) StreamBackfillData(table string, snapshotName string, readQuery string) (colsChan chan []db.Column, rowsChan chan [][]any) {
	colsChan = make(chan []db.Column)
	rowsChan = make(chan [][]any)

	var schema, tblName, nullFields string
	schemaAndTable := strings.Split(table, ".")
	if len(schemaAndTable) < 2 {
		schema = "public"
		tblName = schemaAndTable[0]
	} else {
		schema = schemaAndTable[0]
		tblName = schemaAndTable[1]
	}

	row := rc.conn.QueryRow(
		context.Background(),
		`SELECT string_agg(
           CASE WHEN data_type = 'ARRAY' THEN
             'NULL::' || substr(udt_name, 2) || '[] old__' || column_name
           ELSE
             'NULL::' || data_type || ' old__' || column_name
           END,
           ', '
         )
FROM information_schema.columns
WHERE table_schema = $1
  AND table_name = $2`,
		schema,
		tblName,
	)
	err := row.Scan(&nullFields)
	if err != nil {
		panic(err)
	}

	tmpl, err := template.New("inputSql").Parse(readQuery)
	if err != nil {
		panic(err)
	}
	tmplVars := map[string]string{
		"operation": "insert",
		"rows":      fmt.Sprintf("(SELECT *, %s FROM %s) r", nullFields, table),
	}
	sql := new(bytes.Buffer)
	err = tmpl.Execute(sql, tmplVars)

	go func() {
		defer close(colsChan)
		defer close(rowsChan)

		_, err := rc.conn.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			panic(err)
		}
		defer func() {
			_, err := rc.conn.Exec(context.Background(), "ROLLBACK")
			if err != nil {
				panic(err)
			}
		}()

		_, err = rc.conn.Exec(context.Background(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName))
		if err != nil {
			panic(err)
		}

		rows, err := rc.conn.Query(context.Background(), sql.String())
		if err != nil {
			log.Printf("[Postgres Backfiller] Error running query:\n%s\n", sql.String())
			panic(err)
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		columns := make([]db.Column, len(fields))
		for i, field := range fields {
			columns[i] = db.Column{
				Name: field.Name,
				Type: oidToDbType(field.DataTypeOID),
			}
		}
		colsChan <- columns

		rowValues := make([][]any, 0, 1)
		for i := 0; rows.Next(); i++ {
			values, err := rows.Values()
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, values)

			if i >= backfillBatchSize {
				rowsChan <- rowValues
				rowValues = make([][]any, 0, len(rowValues))
				i = 0
			}
		}

		log.Printf("Read a batch of %d raw rows for backfill...\n", len(rowValues))
		rowsChan <- rowValues
	}()

	return colsChan, rowsChan
}
