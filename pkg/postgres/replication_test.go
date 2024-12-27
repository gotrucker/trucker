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
		changesets := make([]*Changeset, 0, 1)
		for changeset := range res {
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

		expectedInsertCols := []string{"id", "name", "old__id", "old__name"}
		if !reflect.DeepEqual(change.Columns, expectedInsertCols) {
			t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, change.Columns)
		}

		expectedInsertValues := [][]any{{json.Number("6"), "Jamaica", nil, nil}}
		if !reflect.DeepEqual(change.Values, expectedInsertValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedInsertValues, change.Values)
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
		changesets := make([]*Changeset, 0, 1)
		for changeset := range res {
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

		expectedUpdateCols := []string{"id", "name", "old__id", "old__name"}
		if !reflect.DeepEqual(change.Columns, expectedUpdateCols) {
			t.Errorf("Expected UpdateCols to be %v but got %v", expectedUpdateCols, change.Columns)
		}

		expectedUpdateValues := [][]any{{json.Number("6"), "Jameca", json.Number("6"), "Jamaica"}}
		if !reflect.DeepEqual(change.Values, expectedUpdateValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedUpdateValues, change.Values)
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
		changesets := make([]*Changeset, 0, 1)
		for changeset := range res {
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

		expectedDeleteCols := []string{"id", "name", "old__id", "old__name"}
		if !reflect.DeepEqual(change.Columns, expectedDeleteCols) {
			t.Errorf("Expected UpdateCols to be %v but got %v", expectedDeleteCols, change.Columns)
		}

		expectedDeleteValues := [][]any{{nil, nil, json.Number("6"), "Jameca"}}
		if !reflect.DeepEqual(change.Values, expectedDeleteValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedDeleteValues, change.Values)
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
