package pg

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestSetup(t *testing.T) {
	conn, rc := replicationTestSetup()
	defer conn.Close()
	defer rc.Stop()

	tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
	if len(tablesToBackfill) != 1 {
		t.Error("Expected to backfill 1 table, but got", len(tablesToBackfill))
	}
	if tablesToBackfill[0] != "public.countries" {
		t.Error("Expected to backfill 'public.countries', but got", tablesToBackfill[0])
	}
	if !strings.Contains(fmt.Sprintf("%v", backfillLSN), "/") {
		t.Error("Expected backfillLSN to be a valid LSN, but got", backfillLSN)
	}
	if !strings.Contains(snapshotName, "-") {
		t.Error("Expected snapshotName to be a valid snapshot name, but got", snapshotName)
	}
}

func TestStart(t *testing.T) {
	conn, rc := replicationTestSetup()
	defer rc.Stop()
	defer conn.Close()

	_, backfillLSN, _ := rc.Setup()

	// Only Jamaica should show up in the replication stream, since everything
	// else is from before the snapthot
	_, err := conn.Exec(
		context.Background(),
		"INSERT INTO public.countries (name) VALUES ('Jamaica')")
	if err != nil {
		t.Error(err)
	}

	changesChan := rc.Start(backfillLSN)

	select {
	case res := <-changesChan:
		if len(res) != 1 {
			t.Error("Expected to receive 1 change, but got", len(res))
		}

		change := res["public.countries"]

		expectedSql := "(VALUES ($1,$2)) AS rows ($3,$4)"
		sql := change.InsertSql.String()
		if sql != expectedSql {
			t.Errorf("Expected InsertSql to be '%s' but got '%s'", expectedSql, sql)
		}

		expectedInsertCols := []string{"id", "name"}
		if !reflect.DeepEqual(change.InsertCols, expectedInsertCols) {
			t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, change.InsertCols)
		}

		expectedValues := []sqlValue{"6", "Jamaica"}
		if !reflect.DeepEqual(change.InsertValues, expectedValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedValues, change.InsertValues)
		}

		if change.UpdateSql != nil || change.UpdateCols != nil || change.UpdateValues != nil {
			t.Error("Expected UpdateSql, UpdateCols, and UpdateValues to be nil")
		}

		if change.DeleteSql != nil || change.DeleteCols != nil || change.DeleteValues != nil {
			t.Error("Expected DeleteSql, DeleteCols, and DeleteValues to be nil")
		}
	case <-time.After(1000 * time.Millisecond):
		t.Error("Reading from replication stream took too long...")
	}
}

func replicationTestSetup() (*pgxpool.Pool, *ReplicationClient) {
	conn := helpers.PrepareTestDb()
	replicationClient := NewReplicationClient([]string{"public.countries"}, helpers.ConnectionCfg)

	return conn, replicationClient
}
