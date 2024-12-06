package integration_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pglogrepl"

	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/pkg/postgres"
	"github.com/tonyfg/trucker/test/helpers"
)

func TestStreamBackfillReadAndWrite(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()
	rc := postgres.NewReplicationClient([]string{"public.whiskies"}, helpers.PostgresCfg)

	r := db.NewReader(
		`SELECT r.id, r.name, r.age, t.name type, c.name country
FROM {{ .rows }}
JOIN public.whisky_types t ON r.whisky_type_id = t.id
JOIN public.countries c ON c.id = t.country_id
ORDER BY r.id`,
		helpers.PostgresCfg,
	)

	w := db.NewWriter(
		"test",
		`INSERT INTO trucker.whiskies_flat (id, name, age, type, country)
SELECT id, name, age, type, country
FROM {{ .rows }}`,
		helpers.ClickhouseCfg,
	)

	_, snapshotLsn, snapshotName := rc.Setup()

	// Jack Daniels isn't supposed to show up in the backfill, since it was
	// added after the snapshot was created. It should be streamed later on...
	_, err := pgConn.Exec(
		context.Background(),
		"INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 7, 1)",
	)
	if err != nil {
		t.Error(err)
	}

	inputChan := rc.StreamBackfillData("public.whiskies", snapshotName)

	for {
		backfillBatch := <-inputChan
		if backfillBatch == nil || len(backfillBatch.Rows) == 0 {
			break
		}

		cols, rows := r.Read("insert", backfillBatch.Columns, backfillBatch.Types, backfillBatch.Rows)
		w.WithTransaction(func() { w.Write(cols, rows) })
	}

	expectedColumns := []string{"id", "name", "age", "type", "country"}
	expectedRows := [][]any{
		{int32(1), "Glenfiddich", int32(15), "Single Malt", "Scotland"},
		{int32(2), "Lagavulin", int32(12), "Triple Distilled", "Ireland"},
		{int32(3), "Hibiki", int32(17), "Japanese", "Japan"},
		{int32(4), "Laphroaig", int32(10), "Salty", "Portugal"},
	}
	columns, rows := loadWhiskiesFlat(t, chConn)

	if !reflect.DeepEqual(expectedColumns, columns) {
		t.Errorf(`Expected
    %T %v,
got %T %v`, expectedColumns, expectedColumns, columns, columns)
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf(`Expected
    %T %v,
got %T %v`, expectedRows, expectedRows, rows, rows)
	}

	fmt.Println("Snapshot LSN", pglogrepl.LSN(snapshotLsn))
	// Now let's stream Jack Daniels
	streamChan := rc.Start(snapshotLsn, 0)

	select {
	case changesets := <-streamChan:
		changeset := changesets["public.whiskies"]

		if len(changeset.UpdateValues) != 0 {
			t.Errorf("Expected 0 updates, got %d", len(changeset.UpdateValues))
		}
		if len(changeset.DeleteValues) != 0 {
			t.Errorf("Expected 0 deletes, got %d", len(changeset.DeleteValues))
		}

		columns, values := r.Read("insert", changeset.InsertColumns, changeset.InsertTypes, changeset.InsertValues)
		w.WithTransaction(func() { w.Write(columns, values) })
	case <-time.After(3 * time.Second):
		t.Error("Reading from channel took too long...")
	}

	rc.Close()
	changesets := <-streamChan
	changeset := changesets["public.whiskies"]
	if changeset != nil {
		t.Error("Expected the channel to be closed, but got", changeset)
	}

	expectedColumns = []string{"id", "name", "age", "type", "country"}
	expectedRows = [][]any{
		{int32(1), "Glenfiddich", int32(15), "Single Malt", "Scotland"},
		{int32(2), "Lagavulin", int32(12), "Triple Distilled", "Ireland"},
		{int32(3), "Hibiki", int32(17), "Japanese", "Japan"},
		{int32(4), "Laphroaig", int32(10), "Salty", "Portugal"},
		{int32(5), "Jack Daniels", int32(7), "Bourbon", "USA"},
	}
	columns, rows = loadWhiskiesFlat(t, chConn)

	if !reflect.DeepEqual(expectedColumns, columns) {
		t.Errorf(`Expected
    %T %v
got %T %v`, expectedColumns, expectedColumns, columns, columns)
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf(`Expected
    %T %v
got %T %v`, expectedRows, expectedRows, rows, rows)
	}
}

func loadWhiskiesFlat(t *testing.T, conn driver.Conn) ([]string, [][]any) {
	rows, err := conn.Query(
		context.Background(),
		"SELECT id, name, age, type, country FROM trucker.whiskies_flat ORDER BY id",
	)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	rowValues := make([][]any, 0, 1)
	for i := 0; rows.Next(); i++ {
		var id, age int32
		var name, whiskyType, country string
		rows.Scan(&id, &name, &age, &whiskyType, &country)
		rowValues = append(rowValues, []any{id, name, age, whiskyType, country})
	}

	return rows.Columns(), rowValues
}
