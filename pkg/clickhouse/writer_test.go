package clickhouse

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tonyfg/trucker/pkg/db"
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
	var lsn uint64
	row.Scan(&lsn)

	if lsn != 0 {
		t.Error("Initial LSN should be zero")
	}

}

func TestSetAndGetCurrentPosition(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()

	lsn := w.GetCurrentPosition()
	if lsn != 0 {
		t.Errorf("Expected empty LSN, got %d", lsn)
	}

	w.SetupPositionTracking()

	lsn = w.GetCurrentPosition()
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

	rows := make(chan [][]any, 1)
	rows <- [][]any{{"1", "Green Spot", 10, "Single Pot Still", "Ireland"}}
	close(rows)
	w.Write(
		&db.ChanChangeset{
			Operation: db.Insert,
			Columns: []db.Column{
				{Name: "id", Type: db.String},
				{Name: "name", Type: db.String},
				{Name: "age", Type: db.Int32},
				{Name: "type", Type: db.String},
				{Name: "country", Type: db.String},
			},
			Rows: rows,
		},
	)

	row := w.conn.QueryRow(
		context.Background(),
		"SELECT id, name, age, type, country FROM trucker.v_whiskies_flat WHERE name = 'Green Spot'")
	var id, name, whiskyType, country string
	var age int32
	row.Scan(&id, &name, &age, &whiskyType, &country)

	if id != "1" {
		t.Error("Expected id = 1, got", id)
	}

	if name != "Green Spot" {
		t.Error("Expected name = 'Green Spot', got", name)
	}

	if age != 20 {
		t.Error("Expected age = 20, got", age)
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

	path := filepath.Join(helpers.Basepath, "../fixtures/projects/postgres_to_clickhouse/truck/output.sql")
	sqlTemplate, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return NewWriter(
		"test",
		string(sqlTemplate),
		helpers.ClickhouseCfg,
	)
}
