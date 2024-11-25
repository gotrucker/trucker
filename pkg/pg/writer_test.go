package pg

import (
	"context"
	"testing"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestSetupLsnTracking(t *testing.T) {
	w := writerTestSetup()
	defer w.conn.Close(context.Background())

	// It should create the LSN tracking table
	w.SetupLsnTracking()
	_, err := w.conn.Exec(
		context.Background(),
		"SELECT * FROM trucker_current_lsn__test")
	if err != nil {
		t.Error(err)
	}

	// If the table already exists that should be ok too...
	w.SetupLsnTracking()
	_, err = w.conn.Exec(
		context.Background(),
		"SELECT * FROM trucker_current_lsn__test")
	if err != nil {
		t.Error(err)
	}

	// - We can write LSNs to the table
	_, err = w.conn.Exec(
		context.Background(),
		"INSERT INTO trucker_current_lsn__test (lsn) VALUES ('123')")
	if err != nil {
		t.Error(err)
	}

	// - We can't add more than 1 row to the table
	_, err = w.conn.Exec(
		context.Background(),
		"INSERT INTO trucker_current_lsn__test (lsn) VALUES ('234')")
	if err == nil {
		t.Error("Expected an error when adding more than 1 row to the LSN tracking table")
	}

	row := w.conn.QueryRow(
		context.Background(),
		"SELECT COUNT(*) FROM trucker_current_lsn__test")
	var count int64
	row.Scan(&count)

	if count > 1 {
		t.Error("Adding more than 1 row to the LSN tracking table should not be possible")
	}
}

func TestGetCurrentLsn(t *testing.T) {
	w := writerTestSetup()
	defer w.conn.Close(context.Background())
	w.SetupLsnTracking()

	lsn := w.GetCurrentLsn()
	if lsn != "" {
		t.Errorf("Expected empty string, got %s", lsn)
	}

	_, err := w.conn.Exec(
		context.Background(),
		"INSERT INTO trucker_current_lsn__test (lsn) VALUES ('123')")
	if err != nil {
		t.Error(err)
	}

	lsn = w.GetCurrentLsn()
	if lsn != "123" {
		t.Errorf("LSN should be '123', got %s", lsn)
	}
}

func TestSetCurrentLsn(t *testing.T) {
	w := writerTestSetup()
	defer w.conn.Close(context.Background())
	w.SetupLsnTracking()
	w.SetCurrentLsn("xxx")

	lsn := w.GetCurrentLsn()
	if lsn != "xxx" {
		t.Errorf("LSN should be 'xxx', got %s", lsn)
	}
}

func TestWrite(t *testing.T) {
	w := writerTestSetup()
	defer w.conn.Close(context.Background())
	w.SetupLsnTracking()

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
	conn := helpers.PrepareTestDb()

	return NewWriter(
		"test",
		"INSERT INTO whiskies (name, age, whisky_type_id) SELECT name, age, whisky_type_id FROM {{.rows}}",
		conn)
}
