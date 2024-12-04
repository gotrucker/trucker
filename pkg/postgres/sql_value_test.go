package postgres

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMakeChangesets(t *testing.T) {
	wal2json := `{"change": [
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"boda4",15,2]}},
{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"a",12,1]},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"boda3",12,1],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"a",12,1]}},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"boda4",15,2],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"b",15,2]}},
{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"b",15,2]},
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"boda5",18,3]}},
{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"c",18,3]},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[1,"boda1",15,4],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"Glenfiddich",15,4]}},
{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"boda5",18,3],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"c",18,3]}},
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"boda1",15,4]}},
{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"boda3",12,1]}}
]}`

	tableChanges := makeChangesets([]byte(wal2json))

	if len(tableChanges) != 1 {
		t.Errorf("Expected 1 table to have changes, got %d", len(tableChanges))
	}

	keys := make([]string, 0, len(tableChanges))
	for k := range tableChanges {
		keys = append(keys, k)
	}
	if keys[0] != "public.whiskies" {
		t.Errorf("Expected table to be 'public.whiskies', got %s", keys[0])
	}

	changes := tableChanges["public.whiskies"]
	if !reflect.DeepEqual(
		changes.InsertColumns,
		[]string{"id", "name", "age", "whisky_type_id", "old__id", "old__name", "old__age", "old__whisky_type_id"},
	) {
		t.Errorf(`Expected InsertColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			changes.InsertColumns)
	}

	expectedInsertVals := [][]any{
		{json.Number("3"), "a", json.Number("12"), json.Number("1"), nil, nil, nil, nil},
		{json.Number("4"), "b", json.Number("15"), json.Number("2"), nil, nil, nil, nil},
		{json.Number("5"), "c", json.Number("18"), json.Number("3"), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(changes.InsertValues, expectedInsertVals) {
		t.Errorf(`Expected InsertValues to be
    %v
got %v`, expectedInsertVals, changes.InsertValues)
	}

	if !reflect.DeepEqual(changes.UpdateColumns, []string{
		"id", "name", "age", "whisky_type_id",
		"old__id", "old__name", "old__age", "old__whisky_type_id",
	}) {
		t.Errorf(
			`Expected UpdateColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			changes.UpdateColumns)
	}

	expectedUpdateVals := [][]any{
		{json.Number("3"), "boda3", json.Number("12"), json.Number("1"), json.Number("3"), "a", json.Number("12"), json.Number("1")},
		{json.Number("4"), "boda4", json.Number("15"), json.Number("2"), json.Number("4"), "b", json.Number("15"), json.Number("2")},
		{json.Number("1"), "boda1", json.Number("15"), json.Number("4"), json.Number("1"), "Glenfiddich", json.Number("15"), json.Number("4")},
		{json.Number("5"), "boda5", json.Number("18"), json.Number("3"), json.Number("5"), "c", json.Number("18"), json.Number("3")},
	}
	if !reflect.DeepEqual(changes.UpdateValues, expectedUpdateVals) {
		t.Errorf(`Expected UpdateValues to be
    %v
got %v`, expectedUpdateVals, changes.UpdateValues)
	}

	if !reflect.DeepEqual(
		changes.DeleteColumns,
		[]string{"old__id", "old__name", "old__age", "old__whisky_type_id", "id", "name", "age", "whisky_type_id"},
	) {
		t.Errorf(
			`Expected DeleteColumns to be
    ['old__id', 'old__name', 'old__age', 'old__whisky_type_id', 'id', 'name', 'age', 'whisky_type_id']
got %v`,
			changes.DeleteColumns)
	}

	expectedDeleteVals := [][]any{
		{json.Number("4"), "boda4", json.Number("15"), json.Number("2"), nil, nil, nil, nil},
		{json.Number("5"), "boda5", json.Number("18"), json.Number("3"), nil, nil, nil, nil},
		{json.Number("1"), "boda1", json.Number("15"), json.Number("4"), nil, nil, nil, nil},
		{json.Number("3"), "boda3", json.Number("12"), json.Number("1"), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(changes.DeleteValues, expectedDeleteVals) {
		t.Errorf(`Expected DeleteValues to be
    %v
got %v`, expectedDeleteVals, changes.DeleteValues)
	}
}
