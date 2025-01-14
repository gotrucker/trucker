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
			{Name: "id", Type: "integer"},
			{Name: "name", Type: "text"},
			{Name: "age", Type: "integer"},
			{Name: "whisky_type_id", Type: "integer"},
		},
	}

	expectedColumns := []db.Column{
		{Name: "id", Type: "integer"},
		{Name: "name", Type: "text"},
		{Name: "age", Type: "integer"},
		{Name: "whisky_type_id", Type: "integer"},
		{Name: "old__id", Type: "integer"},
		{Name: "old__name", Type: "text"},
		{Name: "old__age", Type: "integer"},
		{Name: "old__whisky_type_id", Type: "integer"},
	}

	changesets := make([]*Changeset, 0, 3)
	for changeset := range makeChangesets([]byte(wal2json), columnsCache) {
		changesets = append(changesets, changeset)
	}

	if len(changesets) != 3 {
		t.Errorf("Expected 3 changes, got %d", len(changesets))
	}

	change1 := changesets[0]
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
		{json.Number("4"), "b", json.Number("15"), json.Number("2"), nil, nil, nil, nil},
		{json.Number("5"), "c", json.Number("18"), json.Number("3"), nil, nil, nil, nil},
	}
	if !reflect.DeepEqual(change1.Values, expectedInsertVals) {
		t.Errorf(`Expected InsertValues to be
    %v
got %v`, expectedInsertVals, change1.Values)
	}

	change2 := changesets[1]
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
		{json.Number("1"), "boda1", json.Number("15"), json.Number("4"), json.Number("1"), "Glenfiddich", json.Number("15"), json.Number("4")},
		{json.Number("5"), "boda5", json.Number("18"), json.Number("3"), json.Number("5"), "c", json.Number("18"), json.Number("3")},
	}
	if !reflect.DeepEqual(change2.Values, expectedUpdateVals) {
		t.Errorf(`Expected UpdateValues to be
    %v
got %v`, expectedUpdateVals, change2.Values)
	}

	change3 := changesets[2]
	if change3.Operation != db.Delete {
		t.Errorf("Expected operation to be Delete, got %s", db.OperationStr(change3.Operation))
	}

	if !reflect.DeepEqual(change3.Columns, expectedColumns) {
		t.Errorf(
			`Expected DeleteColumns to be
    ['id', 'name', 'age', 'whisky_type_id', 'old__id', 'old__name', 'old__age', 'old__whisky_type_id']
got %v`,
			change3.Columns)
	}

	expectedDeleteVals := [][]any{
		{nil, nil, nil, nil, json.Number("4"), nil, json.Number("15"), json.Number("2")},
		{nil, nil, nil, nil, json.Number("5"), "boda5", json.Number("18"), json.Number("3")},
		{nil, nil, nil, nil, json.Number("1"), "boda1", json.Number("15"), json.Number("4")},
		{nil, nil, nil, nil, json.Number("3"), "boda3", json.Number("12"), json.Number("1")},
	}
	if !reflect.DeepEqual(change3.Values, expectedDeleteVals) {
		t.Errorf(`Expected DeleteValues to be
    %v
got %v`, expectedDeleteVals, change3.Values)
	}
}
