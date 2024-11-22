package integration_test

import (
	"testing"

	"github.com/tonyfg/trucker/test/helpers"
	"github.com/tonyfg/trucker/pkg/pg"
)

func TestBackfill(t *testing.T) {
	conn := helpers.PrepareTestDb()
	defer r.conn.Close()

	r := pg.NewReader(
		`SELECT r.id, r.name, r.age, t.name type, c.name country
FROM {{ .rows }}
JOIN whisky_types t ON t.id = r.whisky_type_id
JOIN countries c ON c.id = t.country_id`,
		conn,
	)


	columns := []string{"id", "name", "age", "whisky_type_id"}
	rows := [][]any{
		{1, "Glenfiddich", 15, 4},
		{3, "Hibiki", 17, 2},
	}

	valuesLiteral, values := valuesToLiteral(columns, rows)
	cols, vals := r.Read("insert", valuesLiteral.String(), values)

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
