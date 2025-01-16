package postgres

import (
	"context"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/test/helpers"
)

func TestRead(t *testing.T) {
	r := readerTestSetup(`SELECT '{{ .operation }}' op, r.id, r.name, r.age, t.name type
FROM {{ .rows }}
JOIN whisky_types t ON t.id = r.whisky_type_id`)
	defer r.Close()

	columns := []db.Column{
		db.Column{Name: "id", Type: db.Int32},
		db.Column{Name: "name", Type: db.String},
		db.Column{Name: "age", Type: db.Int32},
		db.Column{Name: "whisky_type_id", Type: db.Int32},
	}
	rows := [][]any{
		{1, "Glenfiddich", 15, 4},
		{3, "Hibiki", 17, 2},
	}

	cols, vals := r.Read(db.Insert, columns, rows)

	expectedCols := []db.Column{
		{Name: "op", Type: db.String},
		{Name: "id", Type: db.Int32},
		{Name: "name", Type: db.String},
		{Name: "age", Type: db.Int32},
		{Name: "type", Type: db.String},
	}
	if !reflect.DeepEqual(cols, expectedCols) {
		t.Errorf("Expected columns to be %v, got %v", expectedCols, cols)
	}

	expectedRows := [][]any{
		{"insert", int32(1), "Glenfiddich", int32(15), "Single Malt"},
		{"insert", int32(3), "Hibiki", int32(17), "Japanese"},
	}
	if !reflect.DeepEqual(vals, expectedRows) {
		t.Errorf(`Expected result rows to be:
     %v,
got: %v`, expectedRows, vals)
	}
}

func TestReadTypes(t *testing.T) {
	r := readerTestSetup(`SELECT * FROM {{ .rows }}`)
	defer r.Close()

	var types []string
	row := r.conn.QueryRow(
		context.Background(),
		`SELECT array_agg(CASE WHEN data_type = 'ARRAY' THEN substring(udt_name, 2) || '[]' ELSE data_type END)
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'weird_types'`,
	)
	err := row.Scan(&types)
	if err != nil {
		panic(err)
	}

	rows, err := r.conn.Query(context.Background(), "SELECT * FROM weird_types")
	if err != nil {
		t.Fatalf("Query failed: %v\n", err)
	}

	columns := make([]string, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		columns[i] = field.Name
	}

	rowValues := make([][]any, 0, 1)
	for i := 0; rows.Next(); i++ {
		values, err := rows.Values()
		if err != nil {
			t.Fatalf("Failed to get values: %v\n", err)
		}
		rowValues = append(rowValues, values)
	}

	cols := make([]db.Column, len(columns))
	for i, col := range columns {
		cols[i] = db.Column{Name: col, Type: pgTypeToDbType(types[i])}
	}
	readCols, readRows := r.Read(db.Insert, cols, rowValues)

	expectedReadCols := []db.Column{
		{Name: "a_number", Type: db.Int64},
		{Name: "a_bool", Type: db.Bool},
		{Name: "a_date", Type: db.Date},
		{Name: "an_ip_addr", Type: db.IPAddr},
		{Name: "a_jsonb", Type: db.MapStringToString},
		{Name: "a_ts", Type: db.DateTime},
		{Name: "a_text_array", Type: db.StringArray},
	}
	if !slices.Equal(readCols, expectedReadCols) {
		t.Fatalf(`Expected readCols to be:
     %v
got: %v`, expectedReadCols, readCols)
	}

	if len(readRows) != 2 {
		t.Fatalf("Expected to read 2 rows, but got %d", len(readRows))
	}

	if readRows[0][0].(int64) != 1234567890 {
		t.Fatalf("Expected readRows[0][0] to be 1234567890 but got %T = %v", readRows[0][0], readRows[0][0])
	}

	if readRows[0][1].(bool) != true {
		t.Fatalf("Expected readRows[0][1] to be true but got %T = %v", readRows[0][1], readRows[0][1])
	}

	expectedTime, _ := time.Parse(time.DateOnly, "2020-01-01")
	if readRows[0][2].(time.Time) != expectedTime {
		t.Fatalf("Expected readRows[0][2] to be '2020-01-01' but got %T = %v", readRows[0][2], readRows[0][2])
	}

	expectedPrefix := netip.MustParsePrefix("192.168.0.1/32")
	if readRows[0][3].(netip.Prefix) != expectedPrefix {
		t.Fatalf("Expected readRows[0][3] to be 192.168.0.1/32 but got %T = %v", readRows[0][3], readRows[0][3])
	}

	if !reflect.DeepEqual(readRows[0][4], map[string]any{"key": "value"}) {
		t.Fatalf("Expected readRows[0][4] to have 'key' => 'value' but got %T = %v", readRows[0][4], readRows[0][4])
	}

	expectedTime, _ = time.Parse(time.DateTime, "2020-01-01 00:37:00")
	if readRows[0][5].(time.Time) != expectedTime {
		t.Fatalf("Expected readRows[0][5] to be '2020-01-01 00:37:00' but got %T = %v", readRows[0][5], readRows[0][5])
	}

	if !reflect.DeepEqual(readRows[0][6], []any{"a", "b", "c"}) {
		t.Fatalf("Expected readRows[0][6] to be ['a', 'b', 'c'] but got %T = %v", readRows[0][6], readRows[0][6])
	}

	for i, v := range readRows[1] {
		if v != nil {
			t.Fatalf("Expected readRows[1] to be all nils but got readRows[1][%d] = %v", i, readRows)
		}
	}
}

func readerTestSetup(inputSql string) *Reader {
	helpers.PreparePostgresTestDb().Close(context.Background())
	return NewReader(inputSql, helpers.PostgresCfg)
}
