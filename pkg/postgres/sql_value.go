// Clickhouse values literal
// SELECT * FROM VALUES('column1 Integer, column2 Integer', (1, 2), (3, 4))

package postgres

import (
	"bytes"
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
	Kind         string   `json:"kind"` // insert, update, delete
	Schema       string   `json:"schema"`
	Table        string   `json:"table"`
	ColumnNames  []string `json:"columnnames"`
	ColumnValues []any    `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string `json:"keynames"`
		KeyTypes  []string `json:"keytypes"`
		KeyValues []any    `json:"keyvalues"`
	} `json:"oldkeys"`
}

const (
	Insert uint8 = iota
	Update
	Delete
)

type Changeset struct {
	Table         string
	InsertColumns []string
	InsertValues  [][]any
	UpdateColumns []string
	UpdateValues  [][]any
	DeleteColumns []string
	DeleteValues  [][]any
}

func makeChangesets(wal2jsonChanges []byte) map[string]*Changeset {
	data := WalData{}
	d := json.NewDecoder(bytes.NewReader(wal2jsonChanges))
	d.UseNumber()
	if err := d.Decode(&data); err != nil {
		log.Fatalf("Failed to unmarshal wal2json payload: %v\n", err)
	}

	changesByTable := make(map[string]*Changeset)

	for _, change := range data.Changes {
		table := fmt.Sprintf("%s.%s", change.Schema, change.Table)

		if _, ok := changesByTable[table]; !ok {
			changesByTable[table] = &Changeset{Table: table}
		}
		changeset := changesByTable[table]

		switch change.Kind {
		case "insert":
			changeset.InsertColumns, changeset.InsertValues = appendChanges(
				changeset.InsertColumns, changeset.InsertValues,
				change.ColumnNames, change.ColumnValues,
			)
		case "update":
			oldColumns := addPrefix(change.OldKeys.KeyNames, "old__")
			changeset.UpdateColumns, changeset.UpdateValues = appendChanges(
				changeset.UpdateColumns, changeset.UpdateValues,
				append(change.ColumnNames, oldColumns...),
				append(change.ColumnValues, change.OldKeys.KeyValues...),
			)
		case "delete":
			oldColumns := addPrefix(change.OldKeys.KeyNames, "old__")
			changeset.DeleteColumns, changeset.DeleteValues = appendChanges(
				changeset.DeleteColumns, changeset.DeleteValues,
				oldColumns, change.OldKeys.KeyValues,
			)

		default:
			log.Fatalf("Unknown operation: %s\n", change.Kind)
		}
	}

	return changesByTable
}

func addPrefix(strings []string, prefix string) []string {
	for i, str := range strings {
		strings[i] = prefix + str
	}
	return strings
}

func appendChanges(columns []string, values [][]any, newColumns []string, newValues []any) ([]string, [][]any) {
	if columns == nil {
		columns = newColumns
		values = make([][]any, 1)
		values[0] = newValues
	} else {
		vals := make([]any, len(columns))
		for i, col := range columns {
			for j, newCol := range newColumns {
				if newCol == col {
					vals[i] = newValues[j]
					break
				}
			}
		}
		values = append(values, vals)
	}

	return columns, values
}

func makeValuesLiteral(columns []string, rows [][]any) (valuesLiteral *strings.Builder, values []any) {
	values = make([]any, 0, len(columns)*len(columns[0]))
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

			sb.WriteString(fmt.Sprintf("$%d%s", (i*len(row))+j+1, sqlType(val)))
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
// type name for each type OID. The list of type OIDs exists in
// pgx/pgtype/pgtype.go
// Once we query it we can cache the result safely in a map or something.
// It doesn't change.
func sqlType(value any) string {
	switch value.(type) {
	case int8, int16, int32, uint8, uint16:
		return "::int"
	case int, int64, uint32:
		return "::bigint"
	case json.Number, pgtype.Numeric, uint64:
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
