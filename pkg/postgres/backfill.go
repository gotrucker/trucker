package postgres

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/db"
)

const channelSize = 3
const batchSize = 2000000

func (rc *ReplicationClient) ReadBackfillData(table string, snapshotName string, readQuery string) *db.ChanChangeset {
	var schema, tblName, nullFields string
	schemaAndTable := strings.Split(table, ".")
	if len(schemaAndTable) < 2 {
		schema = "public"
		tblName = schemaAndTable[0]
	} else {
		schema = schemaAndTable[0]
		tblName = schemaAndTable[1]
	}

	ctx := context.Background()
	row := rc.conn.QueryRow(
		ctx,
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
		"operation":   "insert",
		"input_table": table,
		"rows":        fmt.Sprintf("(SELECT *, %s FROM %s) r", nullFields, table),
	}
	sql := new(bytes.Buffer)
	err = tmpl.Execute(sql, tmplVars)

	tx, err := rc.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec(context.Background(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName))
	if err != nil {
		panic(err)
	}

	rows, err := tx.Query(context.Background(), sql.String())
	if err != nil {
		log.Printf("[Postgres Backfiller] Error running query:\n%s\n", sql.String())
		panic(err)
	}

	fields := rows.FieldDescriptions()
	columns := make([]db.Column, len(fields))
	for i, field := range fields {
		// TODO: can we see if a column is nullable here? Passing that information along, would be helpful for dealing with nullable/non-nullable columns further downstream
		columns[i] = db.Column{
			Name: field.Name,
			Type: oidToDbType(field.DataTypeOID),
		}
	}

	rowChan := make(chan [][]any, channelSize)

	// TODO This go routine is basically the same between reader and backfill. Refactor to avoid dups
	go func() {
		defer func() {
			err := tx.Rollback(ctx)
			if err != nil && rc.running {
				panic(err)
			}
		}()
		defer rows.Close()
		defer func() {
			close(rowChan)
		}()

		rowBatch := make([][]any, 0, batchSize/len(columns))

		for rows.Next() {
			row, err := rows.Values()
			if err != nil {
				panic(err)
			}

			rowBatch = append(rowBatch, row)

			if len(rowBatch) >= batchSize/len(columns) {
				rowChan <- rowBatch
				rowBatch = make([][]any, 0, batchSize)
			}
		}

		if len(rowBatch) > 0 {
			rowChan <- rowBatch
		}
	}()

	return &db.ChanChangeset{
		Operation: db.Insert,
		Table:     table,
		Columns:   columns,
		Rows:      rowChan,
	}
}
