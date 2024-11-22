package pg

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUnmarshalJSON(t *testing.T) {
	payload := `{"string": "cool", "int": 2, "float": 3.1, "bool": true, "null": null, "empty string": ""}`

	var data map[string]sqlValue
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		t.Errorf("Failed to unmarshal json: %v", err)
	}

	expected := map[string]sqlValue{
		"string":       "cool",
		"int":          "2",
		"float":        "3.1",
		"bool":         "true",
		"null":         "NULL",
		"empty string": ""}
	if !reflect.DeepEqual(data, expected) {
		t.Errorf("Expected %v, got %v", expected, data)
	}
}

func TestWal2jsonToSqlValues(t *testing.T) {
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

	tableChanges := wal2jsonToSqlValues([]byte(wal2json))

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
	if !reflect.DeepEqual(changes.InsertCols, []string{"id", "name", "age", "whisky_type_id"}) {
		t.Errorf(
			"Expected InsertCols to be ['id', 'name', 'age', 'whisky_type_id'], got %v",
			changes.InsertCols)
	}

	expectedInsertVals := []sqlValue{
		"3", "a", "12", "1",
		"4", "b", "15", "2",
		"5", "c", "18", "3"}
	if !reflect.DeepEqual(changes.InsertValues, expectedInsertVals) {
		t.Errorf("Expected values to be %v, got %v", expectedInsertVals, changes.InsertValues)
	}

	if !reflect.DeepEqual(changes.UpdateCols, []string{
		"id", "name", "age", "whisky_type_id",
		"old__id", "old__name", "old__age", "old__whisky_type_id"}) {
		t.Errorf(
			"Expected UpdateCols to be ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id'], got %v",
			changes.UpdateCols)
	}

	expectedUpdateVals := []sqlValue{
		"3", "boda3", "12", "1", "3", "a", "12", "1",
		"4", "boda4", "15", "2", "4", "b", "15", "2",
		"1", "boda1", "15", "4", "1", "Glenfiddich", "15", "4",
		"5", "boda5", "18", "3", "5", "c", "18", "3"}
	if !reflect.DeepEqual(changes.UpdateValues, expectedUpdateVals) {
		t.Errorf("Expected values to be %v, got %v", expectedUpdateVals, changes.UpdateValues)
	}

	if !reflect.DeepEqual(changes.DeleteCols, []string{"old__id", "old__name", "old__age", "old__whisky_type_id"}) {
		t.Errorf(
			"Expected DeleteCols to be ['old__id', 'old__name', 'old__age', 'old__whisky_type_id'], got %v",
			changes.DeleteCols)
	}

	expectedDeleteVals := []sqlValue{
		"4", "boda4", "15", "2",
		"5", "boda5", "18", "3",
		"1", "boda1", "15", "4",
		"3", "boda3", "12", "1"}

	if !reflect.DeepEqual(changes.DeleteValues, expectedDeleteVals) {
		t.Errorf("Expected values to be %v, got %v", expectedDeleteVals, changes.DeleteValues)
	}
}
