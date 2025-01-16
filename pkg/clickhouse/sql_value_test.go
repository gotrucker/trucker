package clickhouse

import (
	"reflect"
	"testing"

	"github.com/tonyfg/trucker/pkg/db"
)

func TestMakeValuesLiteral(t *testing.T) {
	cols := []db.Column{
		{Name: "a", Type: db.Int64},
		{Name: "b", Type: db.Int64},
	}
	rows := [][]any{{1, 2}, {3, 4}}
	sb, values := makeValuesLiteral(cols, rows)

	expectedValueStr := "VALUES('a Nullable(Int64),b Nullable(Int64)', ($1,$2),($3,$4)) r"
	if sb.String() != expectedValueStr {
		t.Errorf(`Expected:
     %s
got: %s`, expectedValueStr, sb.String())
	}

	if !reflect.DeepEqual(values, []any{1, 2, 3, 4}) {
		t.Errorf("Expected [1,2,3,4], got %v", values)
	}
}
