package postgres

import (
	"testing"

	"github.com/tonyfg/trucker/pkg/db"
)

func TestMakeValuesListFromRowChan(t *testing.T) {
	columns := []db.Column{
		{Name: "a", Type: db.Int32}, {Name: "b", Type: db.Int32}, {Name: "c", Type: db.Int32},
		{Name: "d", Type: db.Int32}, {Name: "e", Type: db.Int32}, {Name: "f", Type: db.Int32},
		{Name: "g", Type: db.Int32}, {Name: "h", Type: db.Int32}, {Name: "i", Type: db.Int32},
		{Name: "j", Type: db.Int32},
	}
	rowChan := make(chan [][]any, 1)
	withTypes := false

	rows := make([][]any, 50)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows := makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 500 {
		t.Error("Expected output of 500 values, but got", len(flatValues))
	}
	if len(extraRows) != 0 {
		t.Error("Expected 0 extra rows, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 3)
	rows = make([][]any, 1000)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rowChan <- rows
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 30000 {
		t.Error("Expected output of 30000 values, but got", len(flatValues))
	}
	if len(extraRows) != 0 {
		t.Error("Expected 0 extra rows, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 1)
	rows = make([][]any, 3276)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 0 {
		t.Error("Expected 0 extra rows, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 2)
	rows = make([][]any, 3200)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rows = make([][]any, 76)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 0 {
		t.Error("Expected 0 extra rows, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 1)
	rows = make([][]any, 3277)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 1 {
		t.Error("Expected 1 extra row, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 2)
	rows = make([][]any, 3200)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rows = make([][]any, 77)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 1 {
		t.Error("Expected 1 extra row, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 2)
	rows = make([][]any, 3276)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rows = make([][]any, 1)
	rows[0] = []any{
		int32(0), int32(0), int32(0), int32(0), int32(0),
		int32(0), int32(0), int32(0), int32(0), int32(0),
	}
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 0 {
		t.Error("Expected 0 extra rows, but got", len(extraRows))
	}
	if unreadRows := <-rowChan; len(unreadRows) != 1 {
		t.Error("Expected rowChan to have 1 row, but it had", len(unreadRows))
	}

	rowChan = make(chan [][]any, 4)
	rows = make([][]any, 1000)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rowChan <- rows
	rowChan <- rows
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 724 {
		t.Error("Expected 724 extra rows, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 2)
	rows = make([][]any, 2000)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 724 {
		t.Error("Expected 724 extra rows, but got", len(extraRows))
	}
	if <-rowChan != nil {
		t.Error("Expected rowChan to be empty, but it wasn't")
	}

	rowChan = make(chan [][]any, 2)
	rows = make([][]any, 4000)
	for i := range rows {
		rows[i] = []any{
			int32(i), int32(i), int32(i), int32(i), int32(i),
			int32(i), int32(i), int32(i), int32(i), int32(i),
		}
	}
	rowChan <- rows
	rowChan <- rows
	close(rowChan)

	_, flatValues, extraRows = makeValuesListFromRowChan(columns, rowChan, [][]any{}, withTypes)
	if len(flatValues) != 32760 {
		t.Error("Expected output of 32760 values, but got", len(flatValues))
	}
	if len(extraRows) != 724 {
		t.Error("Expected 724 extra rows, but got", len(extraRows))
	}
	if unreadRows := <-rowChan; len(unreadRows) != 4000 {
		t.Error("Expected rowChan to have 4000 rows, but it had", len(unreadRows))
	}
}
