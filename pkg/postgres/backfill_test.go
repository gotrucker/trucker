package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/tonyfg/trucker/pkg/db"
)

func TestStreamBackfillData(t *testing.T) {
	conn, rc := replicationTestSetup("public.countries")
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

	colChan, rowChan := rc.StreamBackfillData("public.countries", snapshotName, "SELECT * FROM {{ .rows }}")
	cols := <-colChan

	expectedInsertCols := []db.Column{
		{Name: "id", Type: db.Int32},
		{Name: "name", Type: db.String},
		{Name: "old__id", Type: db.Int32},
		{Name: "old__name", Type: db.String},
	}
	if !reflect.DeepEqual(cols, expectedInsertCols) {
		t.Errorf("Expected InsertCols to be %v but got %v", expectedInsertCols, cols)
	}

	select {
	case rows := <-rowChan:
		expectedValues := [][]any{
			{int32(1), "Portugal", nil, nil},
			{int32(2), "Scotland", nil, nil},
			{int32(3), "Ireland", nil, nil},
			{int32(4), "Japan", nil, nil},
			{int32(5), "USA", nil, nil},
		}
		if !reflect.DeepEqual(rows, expectedValues) {
			t.Errorf("Expected Values to be %v but got %v", expectedValues, rows)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Reading from channel took too long...")
	}

	select {
	case rows := <-rowChan:
		if rows != nil {
			t.Error("Expected the channel to be closed, but got", rows)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Reading from channel took too long...")
	}
}
