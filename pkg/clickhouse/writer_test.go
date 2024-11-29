package clickhouse

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
	err := w.conn.Exec(context.Background(), "SELECT * FROM trucker_current_lsn__test")
	if err != nil {
		t.Error(err)
	}

	// If the table already exists that should be ok too...
	w.SetupPositionTracking()
	err = w.conn.Exec(context.Background(), "SELECT * FROM trucker_current_lsn__test")
	if err != nil {
		t.Error(err)
	}

	row := w.conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM trucker_current_lsn__test")
	var count uint64
	row.Scan(&count)

	if count != 1 {
		t.Error("There should be exactly 1 row in the LSN tracking view, but found", count)
	}

	row = w.conn.QueryRow(context.Background(), "SELECT lsn FROM trucker_current_lsn__test")
	var lsn int64
	row.Scan(&lsn)

	if lsn != 0 {
		t.Error("Initial LSN should be zero")
	}

}

func TestSetAndGetCurrentPosition(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()
	w.SetupPositionTracking()

	lsn := w.GetCurrentPosition()
	if lsn != 0 {
		t.Errorf("Expected empty LSN, got %d", lsn)
	}

	w.SetCurrentPosition(123)
	lsn = w.GetCurrentPosition()
	if lsn != 123 {
		t.Errorf("LSN should be 123, got %d", lsn)
	}
}

func TestWrite(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()
	w.SetupPositionTracking()

	w.Write(
		[]string{"id", "name", "age", "type", "country"},
		[][]any{{1, "Green Spot", 10, "Single Pot Still", "Ireland"}},
	)
	row := w.conn.QueryRow(
		context.Background(),
		"SELECT id, name, age, type, country FROM whiskies_flat WHERE name = 'Green Spot'")
	var id, age int32
	var name, whiskyType, country string
	row.Scan(&id, &name, &age, &whiskyType, &country)

	if id != 1 {
		t.Error("Expected id = 1, got", id)
	}

	if name != "Green Spot" {
		t.Error("Expected name = 'Green Spot', got", name)
	}

	if age != 10 {
		t.Error("Expected age = 10, got", age)
	}

	if whiskyType != "Single Pot Still" {
		t.Error("Expected whisky_type_id = 'Single Pot Still', got", whiskyType)
	}

	if country != "Ireland" {
		t.Error("Expected country = 'Ireland', got", country)
	}
}

func writerTestSetup() *Writer {
	helpers.PrepareClickhouseTestDb().Close()

	return NewWriter(
		"test",
		"INSERT INTO whiskies_flat (id, name, age, type, country) SELECT id, name, age, type, country FROM {{.rows}}",
		helpers.ClickhouseCfg,
	)
}
