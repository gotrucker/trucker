package clickhouse

import (
	"testing"

	"github.com/tonyfg/trucker/pkg/db"
)

func TestMakeColumnTypesSql(t *testing.T) {
	columns := []db.Column{
		{Name: "id", Type: db.Int32},
		{Name: "name", Type: db.String},
	}

	sb := makeColumnTypesSql(columns)
	expected := "id Int32,name String"
	if sb.String() != expected {
		t.Errorf(`Expected: %s`, expected)
	}
}

func TestDbTypeToChType(t *testing.T) {
	var r string

	if r = dbTypeToChType(db.Int8); r != "Int8" {
		t.Error("Expected Int8, got", r)
	}
	if r = dbTypeToChType(db.Int16); r != "Int16" {
		t.Error("Expected Int16, got", r)
	}
	if r = dbTypeToChType(db.Int32); r != "Int32" {
		t.Error("Expected Int32, got", r)
	}
	if r = dbTypeToChType(db.Int64); r != "Int64" {
		t.Error("Expected Int64, got", r)
	}
	if r = dbTypeToChType(db.UInt8); r != "UInt8" {
		t.Error("Expected UInt8, got", r)
	}
	if r = dbTypeToChType(db.UInt16); r != "UInt16" {
		t.Error("Expected UInt16, got", r)
	}
	if r = dbTypeToChType(db.UInt32); r != "UInt32" {
		t.Error("Expected UInt32, got", r)
	}
	if r = dbTypeToChType(db.UInt64); r != "UInt64" {
		t.Error("Expected UInt64, got", r)
	}
	if r = dbTypeToChType(db.Float32); r != "Float32" {
		t.Error("Expected Float32, got", r)
	}
	if r = dbTypeToChType(db.Float64); r != "Float64" {
		t.Error("Expected Float64, got", r)
	}
	if r = dbTypeToChType(db.Numeric); r != "Decimal" {
		t.Error("Expected Decimal, got", r)
	}
	if r = dbTypeToChType(db.Bool); r != "Boolean" {
		t.Error("Expected Boolean, got", r)
	}
	if r = dbTypeToChType(db.String); r != "String" {
		t.Error("Expected String, got", r)
	}
	if r = dbTypeToChType(db.Date); r != "Date32" {
		t.Error("Expected Date32, got", r)
	}
	if r = dbTypeToChType(db.DateTime); r != "DateTime64" {
		t.Error("Expected DateTime64, got", r)
	}
	if r = dbTypeToChType(db.IPAddr); r != "IPv4" {
		t.Error("Expected IPv4, got", r)
	}
	if r = dbTypeToChType(db.MapStringToString); r != "Map(String, String)" {
		t.Error("Expected Map(String, String), got", r)
	}
	if r = dbTypeToChType(db.Int8Array); r != "Array(Int8)" {
		t.Error("Expected Array(Int8), got", r)
	}
	if r = dbTypeToChType(db.Int16Array); r != "Array(Int16)" {
		t.Error("Expected Array(Int16), got", r)
	}
	if r = dbTypeToChType(db.Int32Array); r != "Array(Int32)" {
		t.Error("Expected Array(Int32), got", r)
	}
	if r = dbTypeToChType(db.Int64Array); r != "Array(Int64)" {
		t.Error("Expected Array(Int64), got", r)
	}
	if r = dbTypeToChType(db.UInt8Array); r != "Array(UInt8)" {
		t.Error("Expected Array(UInt8), got", r)
	}
	if r = dbTypeToChType(db.UInt16Array); r != "Array(UInt16)" {
		t.Error("Expected Array(UInt16), got", r)
	}
	if r = dbTypeToChType(db.UInt32Array); r != "Array(UInt32)" {
		t.Error("Expected Array(UInt32), got", r)
	}
	if r = dbTypeToChType(db.UInt64Array); r != "Array(UInt64)" {
		t.Error("Expected Array(UInt64), got", r)
	}
	if r = dbTypeToChType(db.Float32Array); r != "Array(Float32)" {
		t.Error("Expected Array(Float32), got", r)
	}
	if r = dbTypeToChType(db.Float64Array); r != "Array(Float64)" {
		t.Error("Expected Array(Float64), got", r)
	}
	if r = dbTypeToChType(db.NumericArray); r != "Array(Decimal)" {
		t.Error("Expected Array(Decimal), got", r)
	}
	if r = dbTypeToChType(db.BoolArray); r != "Array(Boolean)" {
		t.Error("Expected Array(Boolean), got", r)
	}
	if r = dbTypeToChType(db.StringArray); r != "Array(String)" {
		t.Error("Expected Array(String), got", r)
	}
	if r = dbTypeToChType(db.DateTimeArray); r != "Array(DateTime64)" {
		t.Error("Expected Array(DateTime64), got", r)
	}
	if r = dbTypeToChType(db.DateArray); r != "Array(Date32)" {
		t.Error("Expected Array(Date32), got", r)
	}
	if r = dbTypeToChType(db.IPAddrArray); r != "Array(IPv4)" {
		t.Error("Expected Array(IPv4), got", r)
	}
	if r = dbTypeToChType(db.MapStringToStringArray); r != "Array(Map(String, String))" {
		t.Error("Expected Array(Map(String, String)), got", r)
	}
	if r = dbTypeToChType(db.FinalValueDoNotUse); r != "String" {
		t.Error("Expected unknown types to be treated as String, got", r)
	}
}

// func TestMakeValuesLiteral(t *testing.T) {
// 	rows := make(chan [][]any, 2)
// 	rows <- [][]any{{1, 2}, {3, 4}}
// 	close(rows)
// 	valuesList, flatValues, _ := makeValuesList(rows, 100000, [][]any{})

// 	expectedValuesList := "($1,$2),($3,$4)"
// 	if valuesList.String() != expectedValuesList {
// 		t.Errorf(`Expected:
//      %s
// got: %s`, expectedValuesList, valuesList.String())
// 	}

// 	if !reflect.DeepEqual(flatValues, []any{1, 2, 3, 4}) {
// 		t.Errorf("Expected [1,2,3,4], got %v", flatValues)
// 	}
// }
