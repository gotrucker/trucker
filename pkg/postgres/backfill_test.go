package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestStreamBackfillData(t *testing.T) {
	conn, rc := replicationTestSetup()
	defer conn.Close(context.Background())
	defer rc.Close()

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
		expectedInsertCols := []string{"id", "name", "old__id", "old__name"}
		if !reflect.DeepEqual(res.Columns, expectedInsertCols) {
			t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, res.Columns)
		}

		expectedValues := [][]any{
			{int32(1), "Portugal", nil, nil},
			{int32(2), "Scotland", nil, nil},
			{int32(3), "Ireland", nil, nil},
			{int32(4), "Japan", nil, nil},
			{int32(5), "USA", nil, nil},
		}
		if !reflect.DeepEqual(res.Rows, expectedValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedValues, res.Rows)
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
