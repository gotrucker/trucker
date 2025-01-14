package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"net/netip"
	"slices"
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
	ColumnValues []any    `json:"columnvalues"`
	OldKeys      struct {
		KeyNames  []string `json:"keynames"`
		KeyValues []any    `json:"keyvalues"`
	} `json:"oldkeys"`
}

type Changeset struct {
	Table     string
	Operation uint8 // Insert, Update, or Delete
	Columns   []db.Column
	Values    [][]any
}

const maxPreparedStatementArgs = 32767

func makeChangesets(wal2jsonChanges []byte, columnsCache map[string][]db.Column) iter.Seq[*Changeset] {
	data := WalData{}
	d := json.NewDecoder(bytes.NewReader(wal2jsonChanges))
	d.UseNumber()
	if err := d.Decode(&data); err != nil {
		log.Fatalf("Failed to unmarshal wal2json payload: %v\n", err)
	}

	changesets := make([]map[string]*Changeset, 3)

	return func(yield func(*Changeset) bool) {
		for _, change := range data.Changes {
			table := fmt.Sprintf("%s.%s", change.Schema, change.Table)

			var operation uint8
			switch change.Kind {
			case "insert":
				operation = db.Insert
			case "update":
				operation = db.Update
			case "delete":
				operation = db.Delete
			default:
				log.Fatalf("Unknown operation: %s\n", change.Kind)
			}

			if changesets[operation] == nil {
				changesets[operation] = make(map[string]*Changeset)
			}
			operationChangesets := changesets[operation]

			if _, ok := operationChangesets[table]; !ok {
				operationChangesets[table] = &Changeset{
					Table: table,
					Operation: operation,
					Columns: changesetCols(columnsCache[table]),
				}
			}
			changeset := operationChangesets[table]

			tableCols := columnsCache[table]
			numCols := len(tableCols)
			values := make([]any, numCols * 2)

			for i, col := range tableCols {
				valueIdx := slices.Index(change.ColumnNames, col.Name)
				if valueIdx > -1 {
					values[i] = change.ColumnValues[i]
				}

				oldValueIdx := slices.Index(change.OldKeys.KeyNames, col.Name)
				if oldValueIdx > -1 {
					values[i+numCols] = change.OldKeys.KeyValues[oldValueIdx]
				}
			}

			changeset.Values = append(changeset.Values, values)

			if len(changeset.Columns) * len(changeset.Values) >= maxPreparedStatementArgs - len(changeset.Columns) {
				if !yield(changeset) {
					return
				}
				delete (changesets[operation], table)
			}
		}

		for _, operationChangesets := range changesets {
			for _, changeset := range operationChangesets {
				if !yield(changeset) {
					return
				}
			}
		}
	}
}

func changesetCols(columns []db.Column) []db.Column {
	cols := make([]db.Column, len(columns)*2)
	for i, col := range columns {
		cols[i] = col
		cols[i+len(columns)] = db.Column{Name: "old__" + col.Name, Type: col.Type}
	}
	return cols
}

func makeValuesLiteral(columns []db.Column, rows [][]any) (valuesLiteral *strings.Builder, values []any) {
	values = make([]any, 0, len(columns)*len(rows))
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

			sb.WriteString(fmt.Sprintf("$%d::%s", (i*len(row))+j+1, columns[j].Type))
			values = append(values, val)
		}

		sb.WriteByte(')')
	}

	sb.WriteString(") AS r (")
	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf(`"%s"`, col.Name))
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
