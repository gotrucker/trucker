package postgres

import (
	"context"
	"testing"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestSetupPositionTracking(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()

	// It should create the LSN tracking table
	w.SetupPositionTracking()
	_, err := w.conn.Exec(
		context.Background(),
		"SELECT * FROM trucker_current_lsn__test")
	if err != nil {
		t.Error(err)
	}

	// If the table already exists that should be ok too...
	w.SetupPositionTracking()
	_, err = w.conn.Exec(
		context.Background(),
		"SELECT * FROM trucker_current_lsn__test")
	if err != nil {
		t.Error(err)
	}

	// - We can write LSNs to the table
	_, err = w.conn.Exec(
		context.Background(),
		"INSERT INTO trucker_current_lsn__test (lsn) VALUES (123)")
	if err != nil {
		t.Error(err)
	}

	// - We can't add more than 1 row to the table
	_, err = w.conn.Exec(
		context.Background(),
		"INSERT INTO trucker_current_lsn__test (lsn) VALUES (234)")
	if err == nil {
		t.Error("Expected an error when adding more than 1 row to the LSN tracking table")
	}

	row := w.conn.QueryRow(
		context.Background(),
		"SELECT COUNT(*) FROM trucker_current_lsn__test")
	var count int64
	row.Scan(&count)

	if count != 1 {
		t.Error("There should be exactly 1 row in the LSN tracking table")
	}
}

func TestGetCurrentPosition(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()
	w.SetupPositionTracking()

	lsn := w.GetCurrentPosition()
	if lsn != 0 {
		t.Errorf("Expected empty LSN, got %d", lsn)
	}

	_, err := w.conn.Exec(
		context.Background(),
		"INSERT INTO trucker_current_lsn__test (lsn) VALUES (123)")
	if err != nil {
		t.Error(err)
	}

	lsn = w.GetCurrentPosition()
	if lsn != 123 {
		t.Errorf("LSN should be 123, got %d", lsn)
	}
}

func TestSetCurrentPosition(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()
	w.SetupPositionTracking()
	w.SetCurrentPosition(8234)

	lsn := w.GetCurrentPosition()
	if lsn != 8234 {
		t.Errorf("LSN should be 8234, got %d", lsn)
	}
}

func TestWrite(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()
	w.SetupPositionTracking()

	w.Write(
		[]string{"name", "age", "whisky_type_id"},
		[][]any{{"Green Spot", 10, 1}},
	)
	row := w.conn.QueryRow(
		context.Background(),
		"SELECT name, age, whisky_type_id FROM whiskies WHERE name = 'Green Spot'")
	var name string
	var age, whiskyTypeId int
	row.Scan(&name, &age, &whiskyTypeId)

	if name != "Green Spot" {
		t.Error("Expected name = 'Green Spot', got", name)
	}

	if age != 10 {
		t.Error("Expected age = 10, got", age)
	}

	if whiskyTypeId != 1 {
		t.Error("Expected whisky_type_id = 1, got", whiskyTypeId)
	}
}

func writerTestSetup() *Writer {
	helpers.PreparePostgresTestDb().Close(context.Background())

	return NewWriter(
		"test",
		"INSERT INTO whiskies (name, age, whisky_type_id) SELECT name, age, whisky_type_id FROM {{.rows}}",
		helpers.PostgresCfg,
	)
}
