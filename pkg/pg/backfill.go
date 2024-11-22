package pg

import (
	"context"
	"fmt"
	"log"
	"strings"
)

const backfillBatchSize = 5000

func (client *ReplicationClient) StreamBackfillData(table string, snapshotName string) chan *TableChanges {
	schemaAndTable := strings.Split(table, ".")
	changesChan := make(chan *TableChanges)

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

		fakeWallChanges := make([]WalChange, 0, backfillBatchSize)

		for i := 0; rows.Next(); i++ {
			values, err := rows.Values()
			if err != nil {
				panic(err)
			}

			sqlValues := make([]sqlValue, len(columns))
			for j := range columns {
				strVal := fmt.Sprintf("%v", values[j])
				sqlValues[j] = sqlValue(strVal)
			}

			fakeWallChanges = append(fakeWallChanges, WalChange{
				Kind: "insert",
				Schema: schemaAndTable[0],
				Table: schemaAndTable[1],
				ColumnNames: columns,
				ColumnValues: sqlValues,
			})

			if i >= backfillBatchSize {
				fakeWalData := &WalData{
					Changes: fakeWallChanges,
				}

				changes := walDataToSqlValues(fakeWalData)[table]
				changesChan <- changes
				i = 0
			}
		}

		fakeWalData := &WalData{
			Changes: fakeWallChanges,
		}

		changes := walDataToSqlValues(fakeWalData)[table]
		changesChan <- changes
	}()

	return changesChan
}
