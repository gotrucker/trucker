// Clickhouse values literal:
// SELECT * FROM VALUES('column1 Integer, column2 Integer', (1, 2), (3, 4))

package clickhouse

import (
	"fmt"
	"log"
	"strings"

	"github.com/tonyfg/trucker/pkg/db"
)

func makeValuesLiteral(columns []db.Column, rows [][]any) (valuesLiteral *strings.Builder, values []any) {
	values = make([]any, 0, len(rows)*len(rows[0]))
	var sb strings.Builder
	sb.WriteString("VALUES('")

	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}

		sb.WriteString(fmt.Sprintf("%s %s", col.Name, dbTypeToChType(col.Type)))
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

func dbTypeToChType(dbType uint8) string {
	switch dbType {
	case db.Int8:
		return "Nullable(Int8)"
	case db.Int16:
		return "Nullable(Int16)"
	case db.Int32:
		return "Nullable(Int32)"
	case db.Int64:
		return "Nullable(Int64)"
	case db.UInt8:
		return "Nullable(UInt8)"
	case db.UInt16:
		return "Nullable(UInt16)"
	case db.UInt32:
		return "Nullable(UInt32)"
	case db.UInt64:
		return "Nullable(UInt64)"
	case db.Float32:
		return "Nullable(Float32)"
	case db.Float64:
		return "Nullable(Float64)"
	case db.Numeric:
		return "Nullable(Decimal)"
	case db.Bool:
		return "Nullable(Boolean)"
	case db.String:
		return "Nullable(String)"
	case db.Date:
		return "Nullable(Date32)"
	case db.DateTime:
		return "Nullable(DateTime64)"
	case db.IPAddr:
		return "Nullable(IPv4)" // FIXME: db.IPAddr can actually be an IPv6...
	case db.MapStringToString:
		return "Nullable(Map(Nullable(String), Nullable(String)))"
	case db.Int8Array:
		return "Array(Nullable(Int8))"
	case db.Int16Array:
		return "Array(Nullable(Int16))"
	case db.Int32Array:
		return "Array(Nullable(Int32))"
	case db.Int64Array:
		return "Array(Nullable(Int64))"
	case db.UInt8Array:
		return "Array(Nullable(UInt8))"
	case db.UInt16Array:
		return "Array(Nullable(UInt16))"
	case db.UInt32Array:
		return "Array(Nullable(UInt32))"
	case db.UInt64Array:
		return "Array(Nullable(UInt64))"
	case db.Float32Array:
		return "Array(Nullable(Float32))"
	case db.Float64Array:
		return "Array(Nullable(Float64))"
	case db.NumericArray:
		return "Array(Nullable(Decimal))"
	case db.BoolArray:
		return "Array(Nullable(Boolean))"
	case db.StringArray:
		return "Array(Nullable(String))"
	case db.DateTimeArray:
		return "Array(Nullable(DateTime64))"
	case db.DateArray:
		return "Array(Nullable(Date32))"
	case db.IPAddrArray:
		return "Array(Nullable(IPv4))" // FIXME: db.IPAddr can actually be an IPv6...
	case db.MapStringToStringArray:
		return "Array(Nullable(Map(Nullable(String), Nullable(String))))"
	default:
		log.Printf("[Clickhouse SQL Value] Unknown type %d, treating as String...\n", dbType)
		return "Nullable(String)"
	}
}
