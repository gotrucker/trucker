package clickhouse

import (
	"reflect"
	"testing"
)

func TestMakeValuesLiteral(t *testing.T) {
	rows := make(chan [][]any, 2)
	rows <- [][]any{{1, 2}, {3, 4}}
	close(rows)
	valuesList, flatValues, _ := makeValuesList(rows, 100000, [][]any{})

	expectedValuesList := "($1,$2),($3,$4)"
	if valuesList.String() != expectedValuesList {
		t.Errorf(`Expected:
     %s
got: %s`, expectedValuesList, valuesList.String())
	}

	if !reflect.DeepEqual(flatValues, []any{1, 2, 3, 4}) {
		t.Errorf("Expected [1,2,3,4], got %v", flatValues)
	}
}
