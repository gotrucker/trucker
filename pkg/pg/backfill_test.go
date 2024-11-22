package pg

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestStreamBackfillData(t *testing.T) {
	conn, rc := replicationTestSetup()
	defer conn.Close()
	defer rc.Stop()

	_, _, snapshotName := rc.Setup()

	// Jamaica isn't supposed to show up in the backfill, since it was added
	// after the snapshot was created
	_, err := conn.Exec(
		context.Background(),
		"INSERT INTO public.countries (name) VALUES ('Jamaica')")
	if err != nil {
		t.Error(err)
	}

	changesChan := rc.StreamBackfillData("public.countries", snapshotName)
	select {
	case res := <-changesChan:
		expectedSql := "(VALUES ($1,$2), ($3,$4), ($5,$6), ($7,$8), ($9,$10)) AS rows ($11,$12)"
		sql := res.InsertSql.String()
		if sql != expectedSql {
			t.Errorf("Expected InsertSql to be '%s' but got '%s'", expectedSql, sql)
		}

		expectedInsertCols := []string{"id", "name"}
		if !reflect.DeepEqual(res.InsertCols, expectedInsertCols) {
			t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, res.InsertCols)
		}

		expectedValues := []sqlValue{"1", "Portugal", "2", "Scotland", "3", "Ireland", "4", "Japan", "5", "USA"}
		if !reflect.DeepEqual(res.InsertValues, expectedValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedValues, res.InsertValues)
		}

		if res.UpdateSql != nil || res.UpdateCols != nil || res.UpdateValues != nil {
			t.Error("Expected UpdateSql, UpdateCols, and UpdateValues to be nil")
		}

		if res.DeleteSql != nil || res.DeleteCols != nil || res.DeleteValues != nil {
			t.Error("Expected DeleteSql, DeleteCols, and DeleteValues to be nil")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Reading from channel took too long...")
	}

	select {
	case res := <-changesChan:
		if res != nil {
			t.Error("Expected the channel to be closed, but got", res)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Reading from channel took too long...")
	}
}
