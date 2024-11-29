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

			// sb.WriteString(fmt.Sprintf("$%d::%s", (i*len(row))+j+1, sqlType(val)))
			sb.WriteString(fmt.Sprintf("$%d", (i*len(row))+j+1))
			values = append(values, val)
		}

		sb.WriteByte(')')
	}
	sb.WriteByte(')')

	return &sb, values
}

// TODO: I'm not sure this is a cheat in clickhouse... Is there a way to query all the types?
func sqlType(value any) string {
	switch value.(type) {
	case int8:
		return "Int8"
	case int16:
		return "Int16"
	case int32:
		return "Int32"
	case int, int64:
		return "Int64"
	case uint8:
		return "UInt8"
	case uint16:
		return "UInt16"
	case uint32:
		return "UInt32"
	case uint, uint64:
		return "UInt64"
	case json.Number, pgtype.Numeric:
		return "Decimal"
	case float32:
		return "Float32"
	case float64:
		return "Float64"
	case time.Time:
		return "DateTime64"
	case bool:
		return "Boolean"
	// FIXME: Clickhouse JSON support is experimental. How do we deal with these?
	// case []any:
	// 	return "json"
	// case map[string]any:
	// 	return "json"
	default:
		return "String"
	}
}
