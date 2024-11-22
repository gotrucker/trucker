package pg

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type sqlValue string

type WalData struct {
	Changes []WalChange `json:"change"`
}

type WalChange struct {
	Kind         string     `json:"kind"` // insert, update, delete
	Schema       string     `json:"schema"`
	Table        string     `json:"table"`
	ColumnNames  []string   `json:"columnnames"`
	ColumnValues []sqlValue `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string   `json:"keynames"`
		KeyTypes  []string   `json:"keytypes"`
		KeyValues []sqlValue `json:"keyvalues"`
	} `json:"oldkeys"`
}

// Parse everything as string, since we just want to feed it to the template
// with no processing at all
// TODO: Test what happens with empty strings
func (v *sqlValue) UnmarshalJSON(data []byte) error {
	s := string(data)

	if strings.ToLower(s) == "null" || strings.ToLower(s) == `"null"` {
		*v = sqlValue("NULL")
	} else if s[0] == '"' {
		// TODO: Should we add single quotes here?
		*v = (sqlValue)(fmt.Sprintf("%s", s[1:len(s)-1]))
	} else {
		*v = sqlValue(s)
	}

	return nil
}

func walDataToSqlValues(data *WalData) map[string]*TableChanges {
	changesByTable := make(map[string]*TableChanges)

	for _, change := range data.Changes {
		table := fmt.Sprintf("%s.%s", change.Schema, change.Table)
		if _, ok := changesByTable[table]; !ok {
			changesByTable[table] = &TableChanges{}
		}
		values := changesByTable[table]

		var sb *strings.Builder
		if change.Kind == "insert" {
			if values.InsertSql == nil {
				values.InsertSql = &strings.Builder{}
			}
			sb = values.InsertSql
		} else if change.Kind == "update" {
			if values.UpdateSql == nil {
				values.UpdateSql = &strings.Builder{}
			}
			sb = values.UpdateSql
		} else if change.Kind == "delete" {
			if values.DeleteSql == nil {
				values.DeleteSql = &strings.Builder{}
			}
			sb = values.DeleteSql
		}

		sbEmpty := sb.Len() == 0
		if sbEmpty {
			sb.WriteString("(VALUES ")
		} else {
			sb.WriteString(", ")
		}

		if change.Kind == "insert" || change.Kind == "update" {
			sb.WriteString("(")
			for j, col := range change.ColumnNames {
				if j > 0 {
					sb.WriteString(",")
				}

				if change.Kind == "insert" {
					if len(values.InsertCols) < len(change.ColumnNames) {
						values.InsertCols = append(values.InsertCols, col)
					}
					values.InsertValues = append(values.InsertValues, change.ColumnValues[j])
					sb.WriteString(fmt.Sprintf("$%d", len(values.InsertValues)))
				} else if change.Kind == "update" {
					if len(values.UpdateCols) < len(change.ColumnNames) {
						values.UpdateCols = append(values.UpdateCols, col)
					}
					values.UpdateValues = append(values.UpdateValues, change.ColumnValues[j])
					sb.WriteString(fmt.Sprintf("$%d", len(values.UpdateValues)))
				}
			}
			sb.WriteString(")")
		}

		if change.Kind == "update" {
			sb.WriteString(", ")
		}

		if change.Kind == "update" || change.Kind == "delete" {
			sb.WriteString("(")
			for j, oldCol := range change.OldKeys.KeyNames {
				if j > 0 {
					sb.WriteString(",")
				}
				colName := fmt.Sprintf("old__%s", oldCol)

				if change.Kind == "update" {
					if len(values.UpdateCols) < len(change.ColumnNames)+len(change.OldKeys.KeyNames) {
						values.UpdateCols = append(values.UpdateCols, colName)
					}
					values.UpdateValues = append(values.UpdateValues, change.OldKeys.KeyValues[j])
					sb.WriteString(fmt.Sprintf("$%d", len(values.UpdateValues)))
				} else if change.Kind == "delete" {
					if len(values.DeleteCols) < len(change.OldKeys.KeyNames) {
						values.DeleteCols = append(values.DeleteCols, colName)
					}
					values.DeleteValues = append(values.DeleteValues, change.OldKeys.KeyValues[j])
					sb.WriteString(fmt.Sprintf("$%d", len(values.DeleteValues)))
				}
			}
			sb.WriteString(")")
		}
	}

	for _, values := range changesByTable {
		if values.InsertSql != nil {
			values.InsertSql.WriteString(") AS rows (")
			for i := range values.InsertCols {
				if i > 0 {
					values.InsertSql.WriteString(",")
				}
				values.InsertSql.WriteString(fmt.Sprintf("$%d", len(values.InsertValues)+i+1))
			}
			values.InsertSql.WriteString(")")
		}

		if values.UpdateSql != nil {
			values.UpdateSql.WriteString(") AS rows (")
			for i := range values.UpdateCols {
				if i > 0 {
					values.UpdateSql.WriteString(",")
				}
				values.UpdateSql.WriteString(fmt.Sprintf("$%d", len(values.UpdateValues)+i+1))
			}
			values.UpdateSql.WriteString(")")
		}

		if values.DeleteSql != nil {
			values.DeleteSql.WriteString(") AS rows (")
			for i := range values.DeleteCols {
				if i > 0 {
					values.DeleteSql.WriteString(",")
				}
				values.DeleteSql.WriteString(fmt.Sprintf("$%d", len(values.DeleteValues)+i+1))
			}
			values.DeleteSql.WriteString(")")
		}
	}

	return changesByTable
}

// Convert wal2json output to a map of changes per table
func wal2jsonToSqlValues(jsonChanges []byte) map[string]*TableChanges {
	data := WalData{}
	if err := json.Unmarshal(jsonChanges, &data); err != nil {
		log.Fatalf("Failed to unmarshal walData: %v\n", err)
	}

	return walDataToSqlValues(&data)
}

func valuesToLiteral(columns []string, rows [][]any) (valuesLiteral *strings.Builder, values []any) {
	values = make([]any, 0, len(columns) * len(columns[0]))
	var sb strings.Builder
	sb.WriteString("(VALUES ")

	for i, row := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')

		for j, val := range row {
			if j > 0 {
				sb.WriteByte(',')
			}

			sb.WriteString(fmt.Sprintf("$%d%s", (i * len(row)) + j + 1, sqlType(val)))
			values = append(values, val)
		}

		sb.WriteByte(')')
	}

	sb.WriteString(") AS r (")
	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf(`"%s"`, col))
	}
	sb.WriteByte(')')

	return &sb, values
}

// TODO: This is a fucking cheat... We need to query the database to get the
//       type name for each type OID. The list of type OIDs exists in
//       pgx/pgtype/pgtype.go
//       Once we query it we can cache the result safely in a map or something.
//       It doesn't change.
func sqlType(value any) string {
	switch value.(type) {
	case int8, int16, int32, uint8, uint16:
		return "::int"
	case int, int64, uint32:
		return "::bigint"
	case pgtype.Numeric, uint64:
		return "::numeric"
	case float32:
		return "::real"
	case float64:
		return "::double precision"
	case time.Time:
		return "::timestamptz"
	case bool:
		return "::bool"
	case []any:
		return "::json"
	case map[string]any:
		return "::json"
	default:
		return ""
	}
}
