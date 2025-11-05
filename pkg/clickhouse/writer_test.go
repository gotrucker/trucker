package clickhouse

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/test/helpers"
)

func TestSetupPositionTracking(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()

	// It should create the LSN tracking table
	w.SetupPositionTracking()
	w.chDo(context.Background(), ch.Query{
		Body:   "SELECT lsn FROM trucker_current_lsn__test2 LIMIT 1",
		Result: proto.AutoResult("lsn"),
	}) // This will crash if the table doesn't exist

	// If the table already exists that should be ok too...
	w.SetupPositionTracking()

	var theLsn proto.ColUInt64
	if err := w.conn.Do(context.Background(), ch.Query{
		Body:   "SELECT max(lsn) lsn FROM trucker_current_lsn__test2",
		Result: proto.Results{{Name: "lsn", Data: &theLsn}},
	}); err != nil {
		t.Error("Failed to query the LSN tracking table", err)
	}
	if theLsn.Row(0) != 0 {
		t.Error("Initial LSN should be zero")
	}
}

func TestSetAndGetCurrentPosition(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()

	lsn := w.GetCurrentPosition()
	if lsn != 0 {
		t.Errorf("Expected LSN to be 0, got %d", lsn)
	}

	w.SetupPositionTracking()

	lsn = w.GetCurrentPosition()
	if lsn != 0 {
		t.Errorf("Expected LSN to be 0, got %d", lsn)
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
	rows <- [][]any{{"1", "Green Spot", int32(10), "Single Pot Still", "Ireland"}}
	close(rows)
	w.Write(&db.ChanChangeset{
		Operation: db.Insert,
		Columns: []db.Column{
			{Name: "id", Type: db.String},
			{Name: "name", Type: db.String},
			{Name: "age", Type: db.Int32},
			{Name: "type", Type: db.String},
			{Name: "country", Type: db.String},
		},
		Rows: rows,
	})

	var id, name, whiskyType, country proto.ColStr
	var age proto.ColInt32
	if err := w.conn.Do(context.Background(), ch.Query{
		Body: "SELECT id, name, age, type, country FROM trucker.v_whiskies_flat WHERE name = 'Green Spot'",
		Result: proto.Results{
			{Name: "id", Data: &id},
			{Name: "name", Data: &name},
			{Name: "age", Data: &age},
			{Name: "type", Data: &whiskyType},
			{Name: "country", Data: &country},
		},
	}); err != nil {
		t.Error("Failed to query v_whiskies_flat", err)
	}

	if id.Row(0) != "1" {
		t.Error("Expected id = 1, got", id)
	}

	if name.Row(0) != "Green Spot" {
		t.Error("Expected name = 'Green Spot', got", name)
	}

	if age.Row(0) != 20 {
		t.Error("Expected age = 20, got", age)
	}

	if whiskyType.Row(0) != "Single Pot Still" {
		t.Error("Expected whisky_type_id = 'Single Pot Still', got", whiskyType)
	}

	if country.Row(0) != "Ireland" {
		t.Error("Expected country = 'Ireland', got", country)
	}
}

func TestWriteZeroRows(t *testing.T) {
	w := writerTestSetup()
	defer w.Close()
	w.SetupPositionTracking()

	rows := make(chan [][]any, 1)
	close(rows)
	result := w.Write(&db.ChanChangeset{
		Operation: db.Insert,
		Columns: []db.Column{
			{Name: "id", Type: db.String},
			{Name: "name", Type: db.String},
			{Name: "age", Type: db.Int32},
			{Name: "type", Type: db.String},
			{Name: "country", Type: db.String},
		},
		Rows: rows,
	})

	if result != false {
		t.Error("Expected Write to return false when no rows are written")
	}

	var cnt proto.ColUInt64
	if err := w.conn.Do(context.Background(), ch.Query{
		Body: "SELECT count(*) AS cnt FROM trucker.v_whiskies_flat",
		Result: proto.Results{
			{Name: "cnt", Data: &cnt},
		},
	}); err != nil {
		t.Error("Failed to query v_whiskies_flat", err)
	}

	if cnt.Row(0) != 0 {
		t.Error("Expected 0 rows, got", cnt)
	}
}

func writerTestSetup() *Writer {
	helpers.PrepareClickhouseTestDb().Close()

	path := filepath.Join(helpers.Basepath, "../fixtures/projects/postgres_to_clickhouse/truck/output.sql")
	sqlTemplate, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return NewWriter("test", string(sqlTemplate), helpers.ClickhouseCfg, "2")
}
