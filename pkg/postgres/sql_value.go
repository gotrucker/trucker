package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"net/netip"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tonyfg/trucker/pkg/db"
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
	ColumnTypes  []string `json:"columntypes"`
	ColumnValues []any    `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string `json:"keynames"`
		KeyTypes  []string `json:"keytypes"`
		KeyValues []any    `json:"keyvalues"`
	} `json:"oldkeys"`
}

type Changeset struct {
	Table     string
	Operation uint8 // Insert, Update, or Delete
	Columns   []string
	Types     []string
	Values    [][]any
}

const maxPreparedStatementArgs = 32767

func makeChangesets(wal2jsonChanges []byte) iter.Seq[*Changeset] {
	data := WalData{}
	d := json.NewDecoder(bytes.NewReader(wal2jsonChanges))
	d.UseNumber()
	if err := d.Decode(&data); err != nil {
		log.Fatalf("Failed to unmarshal wal2json payload: %v\n", err)
	}

	insertsByTable := make(map[string]*Changeset)
	updatesByTable := make(map[string]*Changeset)
	deletesByTable := make(map[string]*Changeset)

	return func(yield func(*Changeset) bool) {
		for _, change := range data.Changes {
			table := fmt.Sprintf("%s.%s", change.Schema, change.Table)

			switch change.Kind {
			case "insert":
				if _, ok := insertsByTable[table]; !ok {
					insertsByTable[table] = &Changeset{Table: table, Operation: db.Insert}
				}
				changeset := insertsByTable[table]

				oldColumns := addPrefix(change.ColumnNames, "old__")
				nils := make([]any, len(change.ColumnNames))

				changeset.Columns, changeset.Types, changeset.Values = appendChanges(
					changeset.Columns, changeset.Types, changeset.Values,
					append(change.ColumnNames, oldColumns...),
					append(change.ColumnTypes, change.ColumnTypes...),
					append(change.ColumnValues, nils...),
				)

				if len(changeset.Columns) * len(changeset.Values) >= maxPreparedStatementArgs - len(changeset.Columns) {
					if !yield(changeset) {
						return
					}
					delete(insertsByTable, table)
				}
			case "update":
				if _, ok := updatesByTable[table]; !ok {
					updatesByTable[table] = &Changeset{Table: table, Operation: db.Update}
				}
				changeset := updatesByTable[table]

				oldColumns := addPrefix(change.OldKeys.KeyNames, "old__")

				changeset.Columns, changeset.Types, changeset.Values = appendChanges(
					changeset.Columns, changeset.Types, changeset.Values,
					append(change.ColumnNames, oldColumns...),
					append(change.ColumnTypes, change.OldKeys.KeyTypes...),
					append(change.ColumnValues, change.OldKeys.KeyValues...),
				)

				if len(changeset.Columns) * len(changeset.Values) >= maxPreparedStatementArgs - len(changeset.Columns) {
					if !yield(changeset) {
						return
					}
					delete(updatesByTable, table)
				}
			case "delete":
				if _, ok := deletesByTable[table]; !ok {
					deletesByTable[table] = &Changeset{Table: table, Operation: db.Delete}
				}
				changeset := deletesByTable[table]

				oldColumns := addPrefix(change.OldKeys.KeyNames, "old__")
				nils := make([]any, len(change.OldKeys.KeyNames))

				changeset.Columns, changeset.Types, changeset.Values = appendChanges(
					changeset.Columns, changeset.Types, changeset.Values,
					append(oldColumns, change.OldKeys.KeyNames...),
					append(change.OldKeys.KeyTypes, change.OldKeys.KeyTypes...),
					append(change.OldKeys.KeyValues, nils...),
				)

				if len(changeset.Columns) * len(changeset.Values) >= maxPreparedStatementArgs - len(changeset.Columns) {
					if !yield(changeset) {
						return
					}
					delete(deletesByTable, table)
				}
			default:
				log.Fatalf("Unknown operation: %s\n", change.Kind)
			}
		}

		for _, changeset := range insertsByTable {
			if !yield(changeset) {
				return
			}
		}
		for _, changeset := range updatesByTable {
			if !yield(changeset) {
				return
			}
		}
		for _, changeset := range deletesByTable {
			if !yield(changeset) {
				return
			}
		}
	}
}

func addPrefix(strings []string, prefix string) []string {
	prefixed := make([]string, len(strings))
	for i, str := range strings {
		prefixed[i] = prefix + str
	}
	return prefixed
}

func appendChanges(columns []string, types []string, values [][]any, newColumns []string, newTypes []string, newValues []any) ([]string, []string, [][]any) {
	if columns == nil {
		columns = newColumns
		types = newTypes
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

	return columns, types, values
}

func makeValuesLiteral(columns []string, types []string, rows [][]any) (valuesLiteral *strings.Builder, values []any) {
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

			sb.WriteString(fmt.Sprintf("$%d::%s", (i*len(row))+j+1, types[j]))
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
// TODO: Well actually we have a mostly correct implementation of something similar in backfill.go. Look there for inspiration
func sqlTypeFromGoValue(value any) string {
	switch value.(type) {
	case int32, uint16:
		return "int"
	case int, int64, uint32:
		return "int8"
	case int8, int16, uint8:
		return "int2"
	case json.Number, pgtype.Numeric, uint64:
		return "numeric"
	case float32:
		return "real"
	case float64:
		return "double precision"
	case time.Time:
		return "timestamptz"
	case bool:
		return "bool"
	case []any:
		return "json"
	case map[string]any:
		return "json"
	case netip.Addr:
		return "inet"
	case netip.Prefix:
		return "cidr"
	default:
		return "text" // FIXME: This isn't really ok is it?
	}
}
