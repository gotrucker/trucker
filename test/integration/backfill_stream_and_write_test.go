package integration_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tonyfg/trucker/test/helpers"
	"github.com/tonyfg/trucker/pkg/pg"
)

func TestStreamBackfillReadAndWrite(t *testing.T) {
	conn := helpers.PrepareTestDb()
	defer conn.Close()
	rc := pg.NewReplicationClient([]string{"public.whiskies"}, helpers.ConnectionCfg)

	r := pg.NewReader(
		`SELECT r.id, r.name, r.age, t.name type, c.name country
FROM {{ .rows }}
JOIN public.whisky_types t ON r.whisky_type_id = t.id
JOIN public.countries c ON c.id = t.country_id`,
		conn,
	)

	w := pg.NewWriter(
		"test",
		`INSERT INTO public.whiskies_flat (id, name, age, type, country)
SELECT id, name, age, type, country
FROM {{ .rows }}`,
		conn,
	)

	_, snapshotLsn, snapshotName := rc.Setup()

	// Jack Daniels isn't supposed to show up in the backfill, since it was
	// added after the snapshot was created. It should be streamed later on...
	_, err := conn.Exec(
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

		cols, rows := r.Read("insert", backfillBatch.Columns, backfillBatch.Rows)
		w.Write(cols, rows)
	}

	expectedColumns := []string{"name", "age", "type", "country"}
	expectedRows := [][]any{
		{"Glenfiddich", int32(15), "Single Malt", "Scotland"},
		{"Lagavulin", int32(12), "Triple Distilled", "Ireland"},
		{"Hibiki", int32(17), "Japanese", "Japan"},
		{"Laphroaig", int32(10), "Salty", "Portugal"},
	}
	columns, rows := loadWhiskiesFlat(t, conn)

	if !reflect.DeepEqual(expectedColumns, columns) {
		t.Errorf("Expected %T %v, got %T %v", expectedColumns, expectedColumns, columns, columns)
	}

	if !reflect.DeepEqual(expectedRows, rows) {
		t.Errorf("Expected %T %v, got %T %v", expectedRows, expectedRows, rows, rows)
	}

	// Now let's stream Jack Daniels
	streamChan := rc.Start(snapshotLsn)

	select {
	case changesets := <-streamChan:
		changeset := changesets["public.whiskies"]

		if len(changeset.UpdateValues) != 0 {
			t.Errorf("Expected 0 updates, got %d", len(changeset.UpdateValues))
		}
		if len(changeset.DeleteValues) != 0 {
			t.Errorf("Expected 0 deletes, got %d", len(changeset.DeleteValues))
		}

		columns, values := r.Read("insert", changeset.InsertColumns, changeset.InsertValues)
		w.Write(columns, values)
	case <-time.After(1 * time.Second):
		t.Error("Reading from channel took too long...")
	}

	rc.Stop()
	changesets := <-streamChan
	changeset := changesets["public.whiskies"]
	if changeset != nil {
		t.Error("Expected the channel to be closed, but got", changeset)
	}

	expectedColumns = []string{"name", "age", "type", "country"}
	expectedRows = [][]any{
		{"Glenfiddich", int32(15), "Single Malt", "Scotland"},
		{"Lagavulin", int32(12), "Triple Distilled", "Ireland"},
		{"Hibiki", int32(17), "Japanese", "Japan"},
		{"Laphroaig", int32(10), "Salty", "Portugal"},
		{"Jack Daniels", int32(7), "Bourbon", "USA"},
	}
	columns, rows = loadWhiskiesFlat(t, conn)

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

func loadWhiskiesFlat(t *testing.T, conn *pgxpool.Pool) ([]string, [][]any) {
	rows, err := conn.Query(
		context.Background(),
		"SELECT name, age, type, country FROM public.whiskies_flat",
	)
	if err != nil {
		t.Error(err)
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
	}

	return columns, rowValues
}
