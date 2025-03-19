package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/pkg/postgres"
	"github.com/tonyfg/trucker/pkg/truck"
	"github.com/tonyfg/trucker/test/helpers"
)

const readQuery = `SELECT r.id::text, r.name, r.age, t.name type, c.name country
FROM {{ .rows }}
JOIN public.whisky_types t ON r.whisky_type_id = t.id
JOIN public.countries c ON c.id = t.country_id
ORDER BY r.id`

func TestBackfillReplicationReadAndWrite(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()
	rc := postgres.NewReplicationClient([]string{"public.whiskies"}, helpers.PostgresCfg)

	r := truck.NewReader(
		readQuery,
		helpers.PostgresCfg,
	)

	w := truck.NewWriter(
		"test",
		`INSERT INTO trucker.whiskies_flat (id, name, age, type, country)
SELECT id,
       argMaxState(name, now64()),
       argMaxState((age * 2)::Int32, now64()),
       argMaxState(type, now64()),
       argMaxState(country, now64())
FROM {{ .rows }}
GROUP BY id`,
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

	changeset := rc.ReadBackfillData("public.whiskies", snapshotName, readQuery)
	cols := changeset.Columns

	for {
		rows := <-changeset.Rows
		if rows == nil || len(rows) == 0 {
			break
		}

		rowChan := make(chan [][]any, 1)
		rowChan <- rows
		close(rowChan)

		changeset := &db.ChanChangeset{
			Operation: db.Insert,
			Table:     "public.whiskies",
			Columns:   cols,
			Rows:      rowChan,
		}
		w.Write(changeset)
	}

	expectedColumns := []string{"id", "name", "age", "type", "country"}
	expectedRows := [][]any{
		{"1", "Glenfiddich", int32(30), "Single Malt", "Scotland"},
		{"2", "Lagavulin", int32(24), "Triple Distilled", "Ireland"},
		{"3", "Hibiki", int32(34), "Japanese", "Japan"},
		{"4", "Laphroaig", int32(20), "Salty", "Portugal"},
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

	// TODO: Check that LSN moved forward

	// Now let's stream Jack Daniels
	streamChan := rc.Start(snapshotLsn, 0)
	processedChangeset := false

	select {
	case transaction := <-streamChan:
		for changeset := range transaction.Changesets {
			processedChangeset = true
			if changeset.Operation != db.Insert {
				t.Error("Expected insert operation, got", db.OperationStr(changeset.Operation))
			}

			result := r.Read(changeset)
			w.Write(result)
		}
	case <-time.After(3 * time.Second):
		t.Error("Reading from channel took too long...")
	}

	if !processedChangeset {
		t.Error("Expected to process a changeset, but didn't")
	}

	// TODO: Check that LSN moved forward

	rc.Close()
	changesets := <-streamChan
	if changesets != nil {
		t.Error("Expected the channel to be closed, but got", changesets)
	}

	expectedColumns = []string{"id", "name", "age", "type", "country"}
	expectedRows = [][]any{
		{"1", "Glenfiddich", int32(30), "Single Malt", "Scotland"},
		{"2", "Lagavulin", int32(24), "Triple Distilled", "Ireland"},
		{"3", "Hibiki", int32(34), "Japanese", "Japan"},
		{"4", "Laphroaig", int32(20), "Salty", "Portugal"},
		{"5", "Jack Daniels", int32(14), "Bourbon", "USA"},
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

func loadWhiskiesFlat(t *testing.T, conn *ch.Client) ([]string, [][]any) {
	var id, name, whiskyType, country proto.ColStr
	var age proto.ColInt32
	err := conn.Do(
		context.Background(),
		ch.Query{
			Body: "SELECT id, name, age, type, country FROM trucker.v_whiskies_flat ORDER BY id",
			Result: proto.Results{
				{Name: "id", Data: &id},
				{Name: "name", Data: &name},
				{Name: "age", Data: &age},
				{Name: "type", Data: &whiskyType},
				{Name: "country", Data: &country},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}

	rowValues := make([][]any, 0, 1)
	for i := range id.Rows() {
		rowValues = append(rowValues, []any{id.Row(i), name.Row(i), age.Row(i), whiskyType.Row(i), country.Row(i)})
	}

	return []string{"id", "name", "age", "type", "country"}, rowValues
}
