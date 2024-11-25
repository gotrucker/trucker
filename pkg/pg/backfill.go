package pg

import (
	"context"
	"fmt"
	"log"
)

const backfillBatchSize = 5000

type BackfillBatch struct {
	Columns []string
	Rows    [][]any
}

func (client *ReplicationClient) StreamBackfillData(table string, snapshotName string) chan *BackfillBatch {
	changesChan := make(chan *BackfillBatch)

	go func() {
		defer close(changesChan)

		_, err := client.conn.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			panic(err)
		}
		_, err = client.conn.Exec(context.Background(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName))
		if err != nil {
			panic(err)
		}

		// TODO: Reading rows from the table to then use them again in the input
		//       query is not ideal. Maybe in the future we can find a way to
		//       directly use the table itself on the input query (just for the
		//       backfill of course)
		rows, err := client.conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			log.Fatalf("Query failed: %v\n", err)
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		columns := make([]string, len(fields))
		for i, field := range fields {
			columns[i] = field.Name
		}

		rowValues := make([][]any, 0, 1)

		for i := 0; rows.Next(); i++ {
			values, err := rows.Values()
			if err != nil {
				panic(err)
			}

			rowValues = append(rowValues, values)

			if i >= backfillBatchSize {
				changesChan <- &BackfillBatch{Columns: columns, Rows: rowValues}
				rowValues = make([][]any, 0, len(rowValues))
				i = 0
			}
		}

		changesChan <- &BackfillBatch{Columns: columns, Rows: rowValues}
	}()

	return changesChan
}
