package postgres

import (
	"context"
	"net/netip"
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

func collectRows(change *db.Changes) [][]any {
	var rows [][]any
	for batch := range change.Rows {
		rows = append(rows, batch...)
	}
	return rows
}

func TestStart(t *testing.T) {
	conn, rc := replicationTestSetup("public.countries")
	defer conn.Close(context.Background())
	defer func() { rc.Close(); <-rc.WaitDone() }()

	_, backfillLSN, _ := rc.Setup()

	// Only Jamaica should show up in the replication stream, since everything
	// else is from before the snapshot
	_, err := conn.Exec(
		context.Background(),
		"INSERT INTO public.countries (name) VALUES ('Jamaica')")
	if err != nil {
		t.Error(err)
	}

	ch := make(chan db.Transaction, 10)
	rc.Register(Subscriber{Name: "test", Tables: map[string]bool{"public.countries": true}, Ch: ch})
	rc.Start(backfillLSN, 0)

	select {
	case transaction := <-ch:
		changes := make([]*db.Changes, 0, 1)
		for change := range transaction.Changes {
			changes = append(changes, change)
		}

		if len(changes) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changes))
		}

		change := changes[0]
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

		expectedInsertValues := [][]any{{int32(6), "Jamaica", nil, nil}}
		if rows := collectRows(change); !reflect.DeepEqual(rows, expectedInsertValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedInsertValues, rows)
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
	case transaction := <-ch:
		changes := make([]*db.Changes, 0, 1)
		for change := range transaction.Changes {
			changes = append(changes, change)
		}

		if len(changes) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changes))
		}

		change := changes[0]
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

		expectedUpdateValues := [][]any{{int32(6), "Jameca", int32(6), "Jamaica"}}
		if rows := collectRows(change); !reflect.DeepEqual(rows, expectedUpdateValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedUpdateValues, rows)
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
	case transaction := <-ch:
		changes := make([]*db.Changes, 0, 1)
		for change := range transaction.Changes {
			changes = append(changes, change)
		}

		if len(changes) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changes))
		}

		change := changes[0]
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

		expectedDeleteValues := [][]any{{nil, nil, int32(6), "Jameca"}}
		if rows := collectRows(change); !reflect.DeepEqual(rows, expectedDeleteValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedDeleteValues, rows)
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}
}

func TestStartWithWeirdTypes(t *testing.T) {
	conn, rc := replicationTestSetup("public.weird_types")
	defer conn.Close(context.Background())
	defer func() { rc.Close(); <-rc.WaitDone() }()

	_, backfillLSN, _ := rc.Setup()

	_, err := conn.Exec(
		context.Background(),
		`INSERT INTO public.weird_types (a_number, a_bool, a_date, an_ip_addr, a_jsonb, a_ts, a_text_array)
VALUES (33, false, '2013-12-11', '193.137.213.0/24', '{"some": "thing"}', '2032-10-01T00:00:22Z', '{yo, yo, ma}')`)
	if err != nil {
		t.Error(err)
	}

	ch := make(chan db.Transaction, 10)
	rc.Register(Subscriber{Name: "test", Tables: map[string]bool{"public.weird_types": true}, Ch: ch})
	rc.Start(backfillLSN, 0)

	select {
	case transaction := <-ch:
		changes := make([]*db.Changes, 0, 1)
		for change := range transaction.Changes {
			changes = append(changes, change)
		}

		if len(changes) != 1 {
			t.Error("Expected to receive 1 change, but got", len(changes))
		}

		change := changes[0]
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

		rows := collectRows(change)
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row but got %d", len(rows))
		}
		row := rows[0]
		// pgtype decodes to native Go types
		if row[0] != int64(33) {
			t.Errorf("Expected a_number=int64(33), got %T(%v)", row[0], row[0])
		}
		if row[1] != false {
			t.Errorf("Expected a_bool=false, got %T(%v)", row[1], row[1])
		}
		if _, ok := row[2].(time.Time); !ok {
			t.Errorf("Expected a_date to be time.Time, got %T", row[2])
		}
		expectedPrefix := netip.MustParsePrefix("193.137.213.0/24")
		if row[3].(netip.Prefix) != expectedPrefix {
			t.Errorf("Expected an_ip_addr to be 193.137.213.0/24, got %T(%v)", row[3], row[3])
		}
		if _, ok := row[4].(map[string]interface{}); !ok {
			t.Errorf("Expected a_jsonb to be map[string]interface{}, got %T", row[4])
		}
		if _, ok := row[5].(time.Time); !ok {
			t.Errorf("Expected a_ts to be time.Time, got %T", row[5])
		}
		if _, ok := row[6].([]interface{}); !ok {
			t.Errorf("Expected a_text_array to be []interface{}, got %T", row[6])
		}
		// old__ columns should all be nil for inserts
		for i := 7; i < 14; i++ {
			if row[i] != nil {
				t.Errorf("Expected old__ column %d to be nil, got %v", i, row[i])
			}
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}
}

func replicationTestSetup(table string) (*pgx.Conn, *ReplicationClient) {
	conn := helpers.PreparePostgresTestDb()
	replicationClient := NewReplicationClient([]string{table}, helpers.PostgresCfg, "2")

	return conn, replicationClient
}
