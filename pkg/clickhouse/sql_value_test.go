package clickhouse

import (
	"reflect"
	"testing"
)

func TestMakeValuesLiteral(t *testing.T) {
	sb, values := makeValuesLiteral([]string{"a", "b"}, [][]any{{1, 2}, {3, 4}})

	if sb.String() != "VALUES('a Int64,b Int64', ($1,$2),($3,$4))" {
		t.Errorf(`Expected "VALUES('a Int64,b Int64', ($1,$2),($3,$4))", got %s`, sb.String())
	}

	if !reflect.DeepEqual(values, []any{1, 2, 3, 4}) {
		t.Errorf("Expected [1,2,3,4], got %v", values)
	}
}
