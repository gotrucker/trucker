// Clickhouse values literal:
// SELECT * FROM VALUES('column1 Integer, column2 Integer', (1, 2), (3, 4))

package clickhouse

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func makeValuesLiteral(columns []string, rows [][]any) (valuesLiteral *strings.Builder, values []any) {
	values = make([]any, 0, len(columns)*len(columns[0]))
	var sb strings.Builder
	sb.WriteString("VALUES('")

	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}

		sb.WriteString(fmt.Sprintf("%s %s", col, sqlType(rows[0][i])))
	}
	sb.WriteString("', ")

	for i, row := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')

		for j, val := range row {
			if j > 0 {
				sb.WriteByte(',')
			}

			sb.WriteString(fmt.Sprintf("$%d", (i*len(row))+j+1))
			values = append(values, val)
		}

		sb.WriteByte(')')
	}
	sb.WriteString(") r")

	return &sb, values
}

// TODO: I'm not sure this is a cheat in clickhouse... Is there a way to query all the types?
// FIXME: This also doesn't wokr when we have nil values... We really need to transport the actual values from the origin DB so we can properly convert NULLs with the correct type
func sqlType(value any) string {
	var theType string

	switch value.(type) {
	case int8:
		theType = "Int8"
	case int16:
		theType = "Int16"
	case int32:
		theType = "Int32"
	case int, int64:
		theType = "Int64"
	case uint8:
		theType = "UInt8"
	case uint16:
		theType = "UInt16"
	case uint32:
		theType = "UInt32"
	case uint, uint64:
		theType = "UInt64"
	case json.Number, pgtype.Numeric:
		theType = "Decimal"
	case float32:
		theType = "Float32"
	case float64:
		theType = "Float64"
	case time.Time:
		theType = "DateTime64"
	case bool:
		theType = "Boolean"
	case []string:
		theType = "Array(String)"
	case []int8:
		theType = "Array(Int8)"
	case []int16:
		theType = "Array(Int16)"
	case []int32:
		theType = "Array(Int32)"
	case []int, []int64:
		theType = "Array(Int64)"
	case []uint8:
		theType = "Array(UInt8)"
	case []uint16:
		theType = "Array(UInt16)"
	case []uint32:
		theType = "Array(UInt32)"
	case []uint, []uint64:
		theType = "Array(UInt64)"
	case []json.Number, []pgtype.Numeric:
		theType = "Array(Decimal)"
	case []float32:
		theType = "Array(Float32)"
	case []float64:
		theType = "Array(Float64)"
	case []time.Time:
		theType = "Array(DateTime64)"
	case []bool:
		theType = "Array(Boolean)"
	// FIXME: Clickhouse JSON support is experimental. How do we deal with these?
	// case []any:
	// 	theType = "json"
	// case map[string]any:
	// 	theType = "json"
	default:
		theType = "String"
	}

	if value == nil {
		theType = "Nullable(" + theType + ")"
	}

	return theType
}
