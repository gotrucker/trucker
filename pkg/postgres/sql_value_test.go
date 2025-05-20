package postgres

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tonyfg/trucker/pkg/db"
)

func TestMakeChangesets(t *testing.T) {
	wal2json := `{"change": [
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","age","whisky_type_id"],"keytypes":["integer","integer","integer"],"keyvalues":[4,15,2]}},
{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"a",12,1]},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"boda3",12,1],"oldkeys":{"keynames":["id","age","whisky_type_id"],"keytypes":["integer","integer","integer"],"keyvalues":[3,12,1]}},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"boda4",15,2],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"b",15,2]}},
{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"b",15,2]},
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"boda5",18,3]}},
{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"c",18,3]},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[1,"boda1",15,4],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"Glenfiddich",15,4]}},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"boda5",18,3],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"c",18,3]}},
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"boda1",15,4]}},
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"boda3",12,1]}}
]}`
	var columnsCache = map[string][]db.Column{
		"public.whiskies": {
			{Name: "id", Type: db.Int32},
			{Name: "name", Type: db.String},
			{Name: "age", Type: db.Int32},
			{Name: "whisky_type_id", Type: db.Int32},
		},
	}

	expectedColumns := []db.Column{
		{Name: "id", Type: db.Int32},
		{Name: "name", Type: db.String},
		{Name: "age", Type: db.Int32},
		{Name: "whisky_type_id", Type: db.Int32},
		{Name: "old__id", Type: db.Int32},
		{Name: "old__name", Type: db.String},
		{Name: "old__age", Type: db.Int32},
		{Name: "old__whisky_type_id", Type: db.Int32},
	}

	changesets := make([]*db.Changeset, 0, 8)
	for changeset := range makeChangesets([]byte(wal2json), columnsCache) {
		changesets = append(changesets, changeset)
	}

	if len(changesets) != 8 {
		t.Errorf("Expected 8 changes, got %d", len(changesets))
	}

	change0 := changesets[0]
	if change0.Operation != db.Delete {
		t.Errorf("Expected operation to be Delete, got %s", db.OperationStr(change0.Operation))
	}

	if !reflect.DeepEqual(change0.Columns, expectedColumns) {
		t.Errorf(
			`Expected DeleteColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change0.Columns)
	}

	expectedDeleteVals := [][]any{
		{nil, nil, nil, nil, json.Number("4"), nil, json.Number("15"), json.Number("2")},
	}
	if !reflect.DeepEqual(change0.Rows, expectedDeleteVals) {
		t.Errorf(`Expected DeleteValues to be
    %v
got %v`, expectedDeleteVals, change0.Rows)
	}

	change1 := changesets[1]
	if change1.Operation != db.Insert {
		t.Errorf("Expected operation to be Insert, got %s", db.OperationStr(change1.Operation))
	}

	if change1.Table != "public.whiskies" {
		t.Errorf("Expected table to be 'public.whiskies', got %s", change1.Table)
	}

	if !reflect.DeepEqual(change1.Columns, expectedColumns) {
		t.Errorf(`Expected InsertColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change1.Columns)
	}

	expectedInsertVals := [][]any{
		{json.Number("3"), "a", json.Number("12"), json.Number("1"), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(change1.Rows, expectedInsertVals) {
		t.Errorf(`Expected InsertValues to be
    %v
got %v`, expectedInsertVals, change1.Rows)
	}

	change2 := changesets[2]
	if change2.Operation != db.Update {
		t.Errorf("Expected operation to be Update, got %s", db.OperationStr(change2.Operation))
	}

	if !reflect.DeepEqual(change2.Columns, expectedColumns) {
		t.Errorf(
			`Expected UpdateColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__age', 'old__whisky_type_id', 'old__name']
got %v`,
			change2.Columns)
	}

	expectedUpdateVals := [][]any{
		{json.Number("3"), "boda3", json.Number("12"), json.Number("1"), json.Number("3"), nil, json.Number("12"), json.Number("1")},
		{json.Number("4"), "boda4", json.Number("15"), json.Number("2"), json.Number("4"), "b", json.Number("15"), json.Number("2")},
	}
	if !reflect.DeepEqual(change2.Rows, expectedUpdateVals) {
		t.Errorf(`Expected UpdateValues to be
    %v
got %v`, expectedUpdateVals, change2.Rows)
	}

	change3 := changesets[3]
	if change3.Operation != db.Insert {
		t.Errorf("Expected operation to be Insert, got %s", db.OperationStr(change3.Operation))
	}

	if change3.Table != "public.whiskies" {
		t.Errorf("Expected table to be 'public.whiskies', got %s", change3.Table)
	}

	if !reflect.DeepEqual(change3.Columns, expectedColumns) {
		t.Errorf(`Expected InsertColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change3.Columns)
	}

	expectedInsertVals = [][]any{
		{json.Number("4"), "b", json.Number("15"), json.Number("2"), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(change3.Rows, expectedInsertVals) {
		t.Errorf(`Expected InsertValues to be
    %v
got %v`, expectedInsertVals, change3.Rows)
	}

	change4 := changesets[4]
	if change4.Operation != db.Delete {
		t.Errorf("Expected operation to be Delete, got %s", db.OperationStr(change4.Operation))
	}

	if !reflect.DeepEqual(change4.Columns, expectedColumns) {
		t.Errorf(
			`Expected DeleteColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change4.Columns)
	}

	expectedDeleteVals = [][]any{
		{nil, nil, nil, nil, json.Number("5"), "boda5", json.Number("18"), json.Number("3")},
	}
	if !reflect.DeepEqual(change4.Rows, expectedDeleteVals) {
		t.Errorf(`Expected DeleteValues to be
    %v
got %v`, expectedDeleteVals, change4.Rows)
	}

	change5 := changesets[5]
	if change5.Operation != db.Insert {
		t.Errorf("Expected operation to be Insert, got %s", db.OperationStr(change5.Operation))
	}

	if change5.Table != "public.whiskies" {
		t.Errorf("Expected table to be 'public.whiskies', got %s", change5.Table)
	}

	if !reflect.DeepEqual(change5.Columns, expectedColumns) {
		t.Errorf(`Expected InsertColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change5.Columns)
	}

	expectedInsertVals = [][]any{
		{json.Number("5"), "c", json.Number("18"), json.Number("3"), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(change5.Rows, expectedInsertVals) {
		t.Errorf(`Expected InsertValues to be
    %v
got %v`, expectedInsertVals, change5.Rows)
	}

	change6 := changesets[6]
	if change6.Operation != db.Update {
		t.Errorf("Expected operation to be Update, got %s", db.OperationStr(change6.Operation))
	}

	if !reflect.DeepEqual(change6.Columns, expectedColumns) {
		t.Errorf(
			`Expected UpdateColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__age', 'old__whisky_type_id', 'old__name']
got %v`,
			change6.Columns)
	}

	expectedUpdateVals = [][]any{
		{json.Number("1"), "boda1", json.Number("15"), json.Number("4"), json.Number("1"), "Glenfiddich", json.Number("15"), json.Number("4")},
		{json.Number("5"), "boda5", json.Number("18"), json.Number("3"), json.Number("5"), "c", json.Number("18"), json.Number("3")},
	}
	if !reflect.DeepEqual(change6.Rows, expectedUpdateVals) {
		t.Errorf(`Expected UpdateValues to be
    %v
got %v`, expectedUpdateVals, change6.Rows)
	}

	change7 := changesets[7]
	if change7.Operation != db.Delete {
		t.Errorf("Expected operation to be Delete, got %s", db.OperationStr(change7.Operation))
	}

	if !reflect.DeepEqual(change7.Columns, expectedColumns) {
		t.Errorf(
			`Expected DeleteColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change7.Columns)
	}

	expectedDeleteVals = [][]any{
		{nil, nil, nil, nil, json.Number("1"), "boda1", json.Number("15"), json.Number("4")},
		{nil, nil, nil, nil, json.Number("3"), "boda3", json.Number("12"), json.Number("1")},
	}
	if !reflect.DeepEqual(change7.Rows, expectedDeleteVals) {
		t.Errorf(`Expected DeleteValues to be
    %v
got %v`, expectedDeleteVals, change7.Rows)
	}
}

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
	if len(extraRows) != 1 {
		t.Error("Expected 1 extra rows, but got", len(extraRows))
	}
	if unreadRows := <-rowChan; len(unreadRows) != 0 {
		t.Error("Expected rowChan to be empty, but it had", len(unreadRows))
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
