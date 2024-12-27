package postgres

import (
	"context"
	"reflect"
	"testing"

	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/test/helpers"
)

func TestRead(t *testing.T) {
	r := readerTestSetup(`SELECT '{{ .operation }}' op, r.id, r.name, r.age, t.name type
FROM {{ .rows }}
JOIN whisky_types t ON t.id = r.whisky_type_id`)
	defer r.Close()

	columns := []string{"id", "name", "age", "whisky_type_id"}
	types := []string{"int8", "text", "int8", "int8"}
	rows := [][]any{
		{1, "Glenfiddich", 15, 4},
		{3, "Hibiki", 17, 2},
	}

	cols, vals := r.Read(db.Insert, columns, types, rows)

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

func TestReadTypes(t *testing.T) {
	r := readerTestSetup(`SELECT * FROM {{ .rows }}`)
	defer r.Close()

	var types []string
	row := r.conn.QueryRow(
		context.Background(),
		`SELECT array_agg(data_type)
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

	r.Read(db.Insert, columns, types, rowValues)
}

func readerTestSetup(inputSql string) *Reader {
	helpers.PreparePostgresTestDb().Close(context.Background())
	return NewReader(inputSql, helpers.PostgresCfg,)
}
