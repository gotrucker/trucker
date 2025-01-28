// Clickhouse values literal:
// SELECT * FROM VALUES('column1 Integer, column2 Integer', (1, 2), (3, 4))

package clickhouse

import (
	"fmt"
	"log"
	"strings"

	"github.com/tonyfg/trucker/pkg/db"
)

func makeColumnTypesSql(columns []db.Column) *strings.Builder {
	var sb strings.Builder
	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("%s %s", col.Name, dbTypeToChType(col.Type)))
	}

	return &sb
}

func makeValuesList(rows [][]any, maxSize int) (valuesList *strings.Builder, values []any) {
	var sb strings.Builder
	values = make([]any, 0, len(rows)*len(rows[0]))
	var rowSize int

	for _, value := range rows[0] {
		rowSize += len(fmt.Sprintf("%v", value))
	}

	totalValueSize := 0
	for i, row := range rows {
		bytesWritten := 0
		if i > 0 {
			sb.WriteByte(',')
			bytesWritten++
		}
		sb.WriteByte('(')
		bytesWritten++

		for j := range len(rows[0]) {
			if j > 0 {
				sb.WriteByte(',')
				bytesWritten++
			}

			s := fmt.Sprintf("$%d", (i*len(rows[0]))+j+1)
			sb.WriteString(s)
			bytesWritten += len(s)
		}

		sb.WriteByte(')')
		values = append(values, row...)
		bytesWritten += rowSize + 1
		totalValueSize += rowSize

		if sb.Len()+totalValueSize+bytesWritten+rowSize > maxSize {
			break
		}
	}

	return &sb, values
}

// FIXME: This needs to return either the regular types or nullable types depending on whether we have null values... :/
func dbTypeToChType(dbType uint8) string {
	switch dbType {
	case db.Int8:
		return "Int8"
	case db.Int16:
		return "Int16"
	case db.Int32:
		return "Int32"
	case db.Int64:
		return "Int64"
	case db.UInt8:
		return "UInt8"
	case db.UInt16:
		return "UInt16"
	case db.UInt32:
		return "UInt32"
	case db.UInt64:
		return "UInt64"
	case db.Float32:
		return "Float32"
	case db.Float64:
		return "Float64"
	case db.Numeric:
		return "Decimal"
	case db.Bool:
		return "Boolean"
	case db.String:
		return "String"
	case db.Date:
		return "Date32"
	case db.DateTime:
		return "DateTime64"
	case db.IPAddr:
		return "IPv4" // FIXME: db.IPAddr can actually be an IPv6...
	case db.MapStringToString:
		return "Map(String, String)"
	case db.Int8Array:
		return "Array(Int8)"
	case db.Int16Array:
		return "Array(Int16)"
	case db.Int32Array:
		return "Array(Int32)"
	case db.Int64Array:
		return "Array(Int64)"
	case db.UInt8Array:
		return "Array(UInt8)"
	case db.UInt16Array:
		return "Array(UInt16)"
	case db.UInt32Array:
		return "Array(UInt32)"
	case db.UInt64Array:
		return "Array(UInt64)"
	case db.Float32Array:
		return "Array(Float32)"
	case db.Float64Array:
		return "Array(Float64)"
	case db.NumericArray:
		return "Array(Decimal)"
	case db.BoolArray:
		return "Array(Boolean)"
	case db.StringArray:
		return "Array(String)"
	case db.DateTimeArray:
		return "Array(DateTime64)"
	case db.DateArray:
		return "Array(Date32)"
	case db.IPAddrArray:
		return "Array(IPv4)" // FIXME: db.IPAddr can actually be an IPv6...
	case db.MapStringToStringArray:
		return "Array(Map(String, String))"
	default:
		log.Printf("[Clickhouse SQL Value] Unknown type %d, treating as String...\n", dbType)
		return "String"
	}
}
