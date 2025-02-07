package postgres

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/test/helpers"
)

func TestSetup(t *testing.T) {
	conn, rc := replicationTestSetup("public.countries")
	defer conn.Close(context.Background())
	defer rc.Close()

	tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
	if len(tablesToBackfill) != 1 {
		t.Error("Expected to backfill 1 table, but got", len(tablesToBackfill))
	}
	if tablesToBackfill[0] != "public.countries" {
		t.Error("Expected to backfill 'public.countries', but got", tablesToBackfill[0])
	}
	if backfillLSN <= 0 {
		t.Error("Expected backfillLSN to be a valid LSN, but got", backfillLSN)
	}
	if !strings.Contains(snapshotName, "-") {
		t.Error("Expected snapshotName to be a valid snapshot name, but got", snapshotName)
	}
}

func TestStart(t *testing.T) {
	conn, rc := replicationTestSetup("public.countries")
	defer conn.Close(context.Background())
	defer rc.Close()

	_, backfillLSN, _ := rc.Setup()

	// Only Jamaica should show up in the replication stream, since everything
	// else is from before the snapthot
	_, err := conn.Exec(
		context.Background(),
		"INSERT INTO public.countries (name) VALUES ('Jamaica')")
	if err != nil {
		t.Error(err)
	}

	changesChan := rc.Start(backfillLSN, 0)

	select {
	case res := <-changesChan:
		changesets := make([]*db.Changeset, 0, 1)
		for changeset := range res.Changesets {
			changesets = append(changesets, changeset)
		}

		if len(changesets) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changesets))
		}

		change := changesets[0]
		if change.Table != "public.countries" {
			t.Errorf("Expected table to be 'public.countries', but got %s", change.Table)
		}

		if change.Operation != db.Insert {
			t.Errorf("Expected operation to be Insert, but got %s", db.OperationStr(change.Operation))
		}

		expectedInsertCols := []db.Column{
			{Name: "id", Type: db.Int32},
			{Name: "name", Type: db.String},
			{Name: "old__id", Type: db.Int32},
			{Name: "old__name", Type: db.String},
		}
		if !reflect.DeepEqual(change.Columns, expectedInsertCols) {
			t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, change.Columns)
		}

		expectedInsertValues := [][]any{{json.Number("6"), "Jamaica", nil, nil}}
		if !reflect.DeepEqual(change.Rows, expectedInsertValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedInsertValues, change.Rows)
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}

	_, err = conn.Exec(
		context.Background(),
		"UPDATE public.countries SET name = 'Jameca' WHERE name = 'Jamaica'")
	if err != nil {
		t.Error(err)
	}

	select {
	case res := <-changesChan:
		changesets := make([]*db.Changeset, 0, 1)
		for changeset := range res.Changesets {
			changesets = append(changesets, changeset)
		}

		if len(changesets) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changesets))
		}

		change := changesets[0]
		if change.Table != "public.countries" {
			t.Errorf("Expected table to be 'public.countries', but got %s", change.Table)
		}

		if change.Operation != db.Update {
			t.Errorf("Expected operation to be Update, but got %s", db.OperationStr(change.Operation))
		}

		expectedUpdateCols := []db.Column{
			{Name: "id", Type: db.Int32},
			{Name: "name", Type: db.String},
			{Name: "old__id", Type: db.Int32},
			{Name: "old__name", Type: db.String},
		}
		if !reflect.DeepEqual(change.Columns, expectedUpdateCols) {
			t.Errorf("Expected UpdateCols to be %v but got %v", expectedUpdateCols, change.Columns)
		}

		expectedUpdateValues := [][]any{{json.Number("6"), "Jameca", json.Number("6"), "Jamaica"}}
		if !reflect.DeepEqual(change.Rows, expectedUpdateValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedUpdateValues, change.Rows)
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}

	_, err = conn.Exec(
		context.Background(),
		"DELETE FROM public.countries WHERE name = 'Jameca'")
	if err != nil {
		t.Error(err)
	}

	select {
	case res := <-changesChan:
		changesets := make([]*db.Changeset, 0, 1)
		for changeset := range res.Changesets {
			changesets = append(changesets, changeset)
		}

		if len(changesets) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changesets))
		}

		change := changesets[0]
		if change.Table != "public.countries" {
			t.Errorf("Expected table to be 'public.countries', but got %s", change.Table)
		}

		if change.Operation != db.Delete {
			t.Errorf("Expected operation to be Delete, but got %s", db.OperationStr(change.Operation))
		}

		expectedDeleteCols := []db.Column{
			{Name: "id", Type: db.Int32},
			{Name: "name", Type: db.String},
			{Name: "old__id", Type: db.Int32},
			{Name: "old__name", Type: db.String},
		}
		if !reflect.DeepEqual(change.Columns, expectedDeleteCols) {
			t.Errorf("Expected UpdateCols to be %v but got %v", expectedDeleteCols, change.Columns)
		}

		expectedDeleteValues := [][]any{{nil, nil, json.Number("6"), "Jameca"}}
		if !reflect.DeepEqual(change.Rows, expectedDeleteValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedDeleteValues, change.Rows)
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}
}

func TestStartWithWeirdTypes(t *testing.T) {
	conn, rc := replicationTestSetup("public.weird_types")
	defer conn.Close(context.Background())
	defer rc.Close()

	_, backfillLSN, _ := rc.Setup()

	_, err := conn.Exec(
		context.Background(),
		`INSERT INTO public.weird_types (a_number, a_bool, a_date, an_ip_addr, a_jsonb, a_ts, a_text_array)
VALUES (33, false, '2013-12-11', '193.137.213.0/24', '{"some": "thing"}', '2032-10-01T00:00:22Z', '{yo, yo, ma}')`)
	if err != nil {
		t.Error(err)
	}

	changesChan := rc.Start(backfillLSN, 0)

	select {
	case res := <-changesChan:
		changesets := make([]*db.Changeset, 0, 1)
		for changeset := range res.Changesets {
			changesets = append(changesets, changeset)
		}

		if len(changesets) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changesets))
		}

		change := changesets[0]
		if change.Table != "public.weird_types" {
			t.Errorf("Expected table to be 'public.weird_types', but got %s", change.Table)
		}

		if change.Operation != db.Insert {
			t.Errorf("Expected operation to be Insert, but got %s", db.OperationStr(change.Operation))
		}

		expectedInsertCols := []db.Column{
			{Name: "a_number", Type: db.Int64},
			{Name: "a_bool", Type: db.Bool},
			{Name: "a_date", Type: db.Date},
			{Name: "an_ip_addr", Type: db.IPAddr},
			{Name: "a_jsonb", Type: db.MapStringToString},
			{Name: "a_ts", Type: db.DateTime},
			{Name: "a_text_array", Type: db.StringArray},
			{Name: "old__a_number", Type: db.Int64},
			{Name: "old__a_bool", Type: db.Bool},
			{Name: "old__a_date", Type: db.Date},
			{Name: "old__an_ip_addr", Type: db.IPAddr},
			{Name: "old__a_jsonb", Type: db.MapStringToString},
			{Name: "old__a_ts", Type: db.DateTime},
			{Name: "old__a_text_array", Type: db.StringArray},
		}
		if !reflect.DeepEqual(change.Columns, expectedInsertCols) {
			t.Errorf(`Expected InsertCols to be:
     %v
got: %v`, expectedInsertCols, change.Columns)
		}

		expectedInsertValues := [][]any{{
			json.Number("33"),
			false,
			"2013-12-11",
			"193.137.213.0/24",
			`{"some": "thing"}`,
			"2032-10-01 00:00:22",
			`{yo,yo,ma}`,
			nil, nil, nil, nil, nil, nil, nil,
		}}
		if !reflect.DeepEqual(change.Rows, expectedInsertValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedInsertValues, change.Rows)
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}
}

func replicationTestSetup(table string) (*pgx.Conn, *ReplicationClient) {
	conn := helpers.PreparePostgresTestDb()
	replicationClient := NewReplicationClient([]string{table}, helpers.PostgresCfg)

	return conn, replicationClient
}
