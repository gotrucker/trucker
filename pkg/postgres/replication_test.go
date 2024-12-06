package postgres

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestSetup(t *testing.T) {
	conn, rc := replicationTestSetup()
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
	conn, rc := replicationTestSetup()
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

	// TODO: Check types are correct
	select {
	case res := <-changesChan:
		if len(res) != 1 {
			t.Error("Expected to receive 1 change, but got", len(res))
		}
		change := res["public.countries"]

		expectedInsertCols := []string{"id", "name", "old__id", "old__name"}
		if !reflect.DeepEqual(change.InsertColumns, expectedInsertCols) {
			t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, change.InsertColumns)
		}

		expectedInsertValues := [][]any{{json.Number("6"), "Jamaica", nil, nil}}
		if !reflect.DeepEqual(change.InsertValues, expectedInsertValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedInsertValues, change.InsertValues)
		}

		if len(change.UpdateColumns) > 0 || len(change.UpdateValues) > 0 {
			t.Error("Expected UpdateColumns to be empty, but got", change.UpdateColumns)
		}

		if len(change.DeleteColumns) > 0 || len(change.DeleteValues) > 0 {
			t.Error("Expected DeleteColumns to be empty, but got", change.DeleteColumns)
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
		if len(res) != 1 {
			t.Error("Expected to receive 1 change, but got", len(res))
		}
		change := res["public.countries"]

		expectedUpdateCols := []string{"id", "name", "old__id", "old__name"}
		if !reflect.DeepEqual(change.UpdateColumns, expectedUpdateCols) {
			t.Errorf("Expected UpdateCols to be %v but got %v", expectedUpdateCols, change.UpdateColumns)
		}

		expectedUpdateValues := [][]any{{json.Number("6"), "Jameca", json.Number("6"), "Jamaica"}}
		if !reflect.DeepEqual(change.UpdateValues, expectedUpdateValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedUpdateValues, change.UpdateValues)
		}

		if len(change.InsertColumns) > 0 || len(change.InsertValues) > 0 {
			t.Error("Expected InsertColumns to be empty, but got", change.UpdateColumns)
		}

		if len(change.DeleteColumns) > 0 || len(change.DeleteValues) > 0 {
			t.Error("Expected DeleteColumns to be empty, but got", change.DeleteColumns)
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
		if len(res) != 1 {
			t.Error("Expected to receive 1 change, but got", len(res))
		}
		change := res["public.countries"]

		expectedDeleteCols := []string{"old__id", "old__name", "id", "name"}
		if !reflect.DeepEqual(change.DeleteColumns, expectedDeleteCols) {
			t.Errorf("Expected UpdateCols to be %v but got %v", expectedDeleteCols, change.DeleteColumns)
		}

		expectedDeleteValues := [][]any{{json.Number("6"), "Jameca", nil, nil}}
		if !reflect.DeepEqual(change.DeleteValues, expectedDeleteValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedDeleteValues, change.DeleteValues)
		}

		if len(change.InsertColumns) > 0 || len(change.InsertValues) > 0 {
			t.Error("Expected InsertColumns to be empty, but got", change.UpdateColumns)
		}

		if len(change.UpdateColumns) > 0 || len(change.UpdateValues) > 0 {
			t.Error("Expected UpdateColumns to be empty, but got", change.DeleteColumns)
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}
}

func replicationTestSetup() (*pgx.Conn, *ReplicationClient) {
	conn := helpers.PreparePostgresTestDb()
	replicationClient := NewReplicationClient([]string{"public.countries"}, helpers.PostgresCfg)

	return conn, replicationClient
}
