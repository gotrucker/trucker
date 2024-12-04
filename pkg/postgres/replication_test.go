package postgres

import (
	"context"
	"encoding/json"
	"fmt"
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
		for _, row := range change.InsertValues {
			for _, value := range row {
				fmt.Printf("val: %v, type: %T\n", value, value)
			}
		}
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

	// TODO: Test updates and deletes
}

func replicationTestSetup() (*pgx.Conn, *ReplicationClient) {
	conn := helpers.PreparePostgresTestDb()
	replicationClient := NewReplicationClient([]string{"public.countries"}, helpers.PostgresCfg)

	return conn, replicationClient
}
