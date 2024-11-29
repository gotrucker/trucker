package postgres

import (
	"context"
	"reflect"
	"testing"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestRead(t *testing.T) {
	r := readerTestSetup()
	defer r.Close()

	columns := []string{"id", "name", "age", "whisky_type_id"}
	rows := [][]any{
		{1, "Glenfiddich", 15, 4},
		{3, "Hibiki", 17, 2},
	}

	cols, vals := r.Read("insert", columns, rows)

	expectedCols := []string{"op", "id", "name", "age", "type"}
	if !reflect.DeepEqual(cols, expectedCols) {
		t.Errorf("Expected columns to be %v, got %v", expectedCols, cols)
	}

	expectedRows := [][]any{
		{"insert", int64(1), "Glenfiddich", int64(15), "Single Malt"},
		{"insert", int64(3), "Hibiki", int64(17), "Japanese"},
	}
	if !reflect.DeepEqual(vals, expectedRows) {
		t.Errorf("Expected result rows to be %v, got %v", expectedRows, vals)
	}
}

func TestReadWeirdTypes(t *testing.T) {
	r := readerTestSetup()
	defer r.Close()

	rows, err := r.conn.Query(context.Background(), "select * from public.weird_types")
	if err != nil {
		t.Fatalf("Failed to query weird_types: %v", err)
	}

	columns := make([]string, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		columns[i] = field.Name
	}

	values := make([][]any, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("Failed to get values: %v", err)
		}

		values = append(values, vals)
	}

	cols, vals := r.Read("insert", columns, values)

	expectedCols := []string{"op", "id", "name", "age", "type"}
	if !reflect.DeepEqual(cols, expectedCols) {
		t.Errorf("Expected columns to be %v, got %v", expectedCols, cols)
	}

	expectedRows := [][]any{
		{"insert", int64(1), "Glenfiddich", int64(15), "Single Malt"},
		{"insert", int64(3), "Hibiki", int64(17), "Japanese"},
	}
	if !reflect.DeepEqual(vals, expectedRows) {
		t.Errorf("Expected result rows to be %v, got %v", expectedRows, vals)
	}

}

func readerTestSetup() *Reader {
	helpers.PreparePostgresTestDb().Close(context.Background())
	return NewReader(
		`SELECT '{{ .operation }}' op, r.id, r.name, r.age, t.name type
FROM {{ .rows }}
JOIN whisky_types t ON t.id = r.whisky_type_id`,
		helpers.PostgresCfg,
	)
}
