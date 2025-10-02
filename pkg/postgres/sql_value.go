package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jackc/pglogrepl"
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

const maxPreparedStatementArgs = 32767

func processV2(
	walData []byte,
	inStream *bool,
	relations map[uint32]*pglogrepl.RelationMessageV2,
	typeMap *pgtype.Map,
) (*db.Change, bool) {
	logicalMsg, err := pglogrepl.ParseV2(walData, *inStream)
	if err != nil {
		log.Fatalf("Parse logical replication message: %s", err)
	}
	log.Printf("Logical replication message: %T\n", logicalMsg)

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		log.Printf("RelationMessageV2: %s.%s\n", logicalMsg.Namespace, logicalMsg.RelationName)
		relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.
		log.Println("BEGIN")

	case *pglogrepl.CommitMessage:
		log.Println("COMMIT")

	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				values[colName] = val
			}
		}
		log.Printf("insert for xid %d\n", logicalMsg.Xid)
		log.Printf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)
		// TODO CONA ESTAS AQUI: é preciso devolver estes values no formato que a gente definiu como return value.

	case *pglogrepl.UpdateMessageV2:
		// ...
		rel, ok := relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}
		values := map[string]interface{}{}
		for idx, col := range logicalMsg.NewTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				values[colName] = val
			}
		}
		for idx, col := range logicalMsg.OldTuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n': // null
				values[colName] = nil
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := decodeTextColumnData(typeMap, col.Data, rel.Columns[idx].DataType)
				if err != nil {
					log.Fatalln("error decoding column data:", err)
				}
				values[fmt.Sprintf("old_%s", colName)] = val
			}
		}
		log.Printf("update for xid %d\n", logicalMsg.Xid)
		log.Printf("UPDATE %s.%s: %v", rel.Namespace, rel.RelationName, values)
	case *pglogrepl.DeleteMessageV2:
		log.Printf("delete for xid %d\n", logicalMsg.Xid)
		// ...
	case *pglogrepl.TruncateMessageV2:
		log.Printf("truncate for xid %d\n", logicalMsg.Xid)
		// ...

	case *pglogrepl.TypeMessageV2:
		log.Println("TYPE")
	case *pglogrepl.OriginMessage:
		log.Println("ORIGIN")

	case *pglogrepl.LogicalDecodingMessageV2:
		log.Printf("Logical decoding message: %q, %q, %d", logicalMsg.Prefix, logicalMsg.Content, logicalMsg.Xid)

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true
		log.Printf("Stream start message: xid %d, first segment? %d", logicalMsg.Xid, logicalMsg.FirstSegment)
	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		log.Printf("Stream stop message")
	case *pglogrepl.StreamCommitMessageV2:
		log.Printf("Stream commit message: xid %d", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	default:
		log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}

	return nil, true // CONA apaga isto
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func makeChangesets(wal2jsonChanges []byte, columnsCache map[string][]db.Column) iter.Seq[*db.Change] {
	data := WalData{}
	d := json.NewDecoder(bytes.NewReader(wal2jsonChanges))
	d.UseNumber()
	if err := d.Decode(&data); err != nil {
		log.Fatalf("Failed to unmarshal wal2json payload: %v\n", err)
	}

	return func(yield func(*db.Change) bool) {
		var changeset *db.Change = nil

		for _, change := range data.Changes {
			table := fmt.Sprintf("%s.%s", change.Schema, change.Table)
			tableCols := columnsCache[table]
			numCols := len(tableCols)

			if changeset == nil || table != changeset.Table || change.Kind != db.OperationStr(changeset.Operation) {
				if changeset != nil {
					if !yield(changeset) {
						return
					}
				}

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

				changeset = &db.Change{
					Table:     table,
					Operation: operation,
					Columns:   changesetCols(tableCols),
					Rows:      make(chan [][]any),
				}
			}

			row := make([]any, numCols*2)

			for i, col := range tableCols {
				valueIdx := slices.Index(change.ColumnNames, col.Name)
				if valueIdx > -1 {
					if valueIdx < len(change.ColumnValues) {
						row[i] = change.ColumnValues[valueIdx]
					} else {
						// TODO: Remove these logs since the issue was probably fixed
						//       by changing change.ColumnValues[i] to
						//       change.ColumnValues[valueIdx] a couple of lines
						//       above.
						log.Printf("Column %s not found in change data for table %s, skipping...\n", col.Name, table)
						log.Printf("Column names: %v\n", change.ColumnNames)
						log.Printf("Column values: %v\n", change.ColumnValues)
					}
				}

				oldValueIdx := slices.Index(change.OldKeys.KeyNames, col.Name)
				if oldValueIdx > -1 {
					row[i+numCols] = change.OldKeys.KeyValues[oldValueIdx]
				}
			}

			changeset.Rows = append(changeset.Rows, row)
		}

		if changeset != nil && !yield(changeset) {
			return
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

func makeColumnsList(columns []db.Column) (columnsLiteral *strings.Builder) {
	var sb strings.Builder

	for i, col := range columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf(`"%s"`, col.Name))
	}

	return &sb
}

func makeValuesList(columns []db.Column, rows [][]any, withTypes bool) (*strings.Builder, []any) {
	valuesList := &strings.Builder{}
	values := make([]any, 0)
	numCols := len(columns)
	maxRows := (maxPreparedStatementArgs / numCols) - 1

	for i, row := range rows {
		if i > 0 {
			valuesList.WriteByte(',')
		}
		valuesList.WriteByte('(')

		for j := range numCols {
			if j > 0 {
				valuesList.WriteByte(',')
			}

			var val string
			if withTypes {
				val = fmt.Sprintf(
					"$%d::%s",
					(i*numCols)+j+1,
					dbTypeToPgType(columns[j].Type),
				)
			} else {
				val = fmt.Sprintf("$%d", (i*numCols)+j+1)
			}
			valuesList.WriteString(val)
		}

		values = append(values, row...)
		valuesList.WriteByte(')')

		if i >= maxRows {
			break
		}
	}

	return valuesList, values
}

func makeValuesListFromRowChan(columns []db.Column, rowChan chan [][]any, leftoverRows [][]any, withTypes bool) (*strings.Builder, []any, [][]any) {
	maxRows := (maxPreparedStatementArgs / len(columns))
	rows := leftoverRows
	for rowBatch := range rowChan {
		rows = append(rows, rowBatch...)

		if len(rows) >= maxRows {
			sql, params := makeValuesList(columns, rows[:maxRows], withTypes)
			return sql, params, rows[maxRows:]
		}
	}

	rowsToProcess := min(len(rows), maxRows)
	sql, params := makeValuesList(columns, rows[:rowsToProcess], withTypes)
	return sql, params, rows[rowsToProcess:]
}

func oidToDbType(oid uint32) uint8 {
	switch oid {
	case pgtype.Int2OID:
		return db.Int16
	case pgtype.Int4OID:
		return db.Int32
	case pgtype.Int8OID:
		return db.Int64
	case pgtype.Float4OID:
		return db.Float32
	case pgtype.Float8OID:
		return db.Float64
	case pgtype.NumericOID:
		return db.Numeric
	case pgtype.BoolOID:
		return db.Bool
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.QCharOID, pgtype.BPCharOID:
		return db.String
	case pgtype.DateOID:
		return db.Date
	case pgtype.TimestampOID:
		return db.DateTime
	case pgtype.InetOID, pgtype.CIDROID:
		return db.IPAddr
	case pgtype.JSONOID, pgtype.JSONBOID:
		return db.MapStringToString
	case pgtype.Int2ArrayOID:
		return db.Int16Array
	case pgtype.Int4ArrayOID:
		return db.Int32Array
	case pgtype.Int8ArrayOID:
		return db.Int64Array
	case pgtype.Float4ArrayOID:
		return db.Float32Array
	case pgtype.Float8ArrayOID:
		return db.Float64Array
	case pgtype.NumericArrayOID:
		return db.NumericArray
	case pgtype.BoolArrayOID:
		return db.BoolArray
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.QCharArrayOID, pgtype.BPCharArrayOID:
		return db.StringArray
	case pgtype.DateArrayOID:
		return db.DateArray
	case pgtype.TimestampArrayOID:
		return db.DateTimeArray
	case pgtype.InetArrayOID, pgtype.CIDRArrayOID:
		return db.IPAddrArray
	case pgtype.JSONArrayOID, pgtype.JSONBArrayOID: // TODO: hstore doesn't have a stable OID since it's an extension. can we get it from pg_types to use here?
		return db.MapStringToStringArray
	default:
		log.Printf("[Postgres SQL Value] Unknown OID %d, treating as string...\n", oid)
		return db.String
	}
}

func pgTypeToDbType(pgType string) uint8 {
	switch pgType {
	case "smallint", "int2", "smallserial":
		return db.Int16
	case "int", "int4", "serial":
		return db.Int32
	case "bigint", "int8", "bigserial":
		return db.Int64
	case "real", "float4":
		return db.Float32
	case "double precision", "float8":
		return db.Float64
	case "decimal", "numeric":
		return db.Numeric
	case "boolean", "bool":
		return db.Bool
	case "text", "varchar", "character varying", "character", "char", "bpchar":
		return db.String
	case "date":
		return db.Date
	case "timestamp without time zone", "timestamp with time zone", "timestamp", "timestamptz":
		return db.DateTime
	case "inet", "cidr":
		return db.IPAddr
	case "hstore", "json", "jsonb":
		return db.MapStringToString
	case "int2[]":
		return db.Int16Array
	case "int4[]":
		return db.Int32Array
	case "int8[]":
		return db.Int64Array
	case "float4[]":
		return db.Float32Array
	case "float8[]":
		return db.Float64Array
	case "numeric[]":
		return db.NumericArray
	case "bool[]":
		return db.BoolArray
	case "text[]", "varchar[]", "character varying[]", "character[]", "char[]", "bpchar[]":
		return db.StringArray
	case "date[]":
		return db.DateArray
	case "timestamp[]":
		return db.DateTimeArray
	case "inet[]", "cidr[]":
		return db.IPAddrArray
	case "hstore[]", "json[]", "jsonb[]":
		return db.MapStringToStringArray
	default:
		log.Printf("[Postgres SQL Value] Unknown type %s, treating as string...\n", pgType)
		return db.String
	}
}

func dbTypeToPgType(dbType uint8) string {
	switch dbType {
	case db.Int8, db.UInt8, db.Int16:
		return "int2"
	case db.UInt16, db.Int32:
		return "int4"
	case db.UInt32, db.Int64:
		return "int8"
	case db.Float32:
		return "float4"
	case db.Float64:
		return "float8"
	case db.UInt64, db.Numeric:
		return "numeric"
	case db.Bool:
		return "bool"
	case db.String:
		return "text"
	case db.Date:
		return "date"
	case db.DateTime:
		return "timestamp"
	case db.IPAddr:
		return "inet"
	case db.MapStringToString:
		return "jsonb"
	case db.Int8Array, db.UInt8Array, db.Int16Array:
		return "int2[]"
	case db.UInt16Array, db.Int32Array:
		return "int4[]"
	case db.UInt32Array, db.Int64Array:
		return "int8[]"
	case db.Float32Array:
		return "float4[]"
	case db.Float64Array:
		return "float8[]"
	case db.UInt64Array, db.NumericArray:
		return "numeric[]"
	case db.BoolArray:
		return "bool[]"
	case db.StringArray:
		return "text[]"
	case db.DateArray:
		return "date[]"
	case db.DateTimeArray:
		return "timestamp[]"
	case db.IPAddrArray:
		return "inet[]"
	case db.MapStringToStringArray:
		return "jsonb[]"
	default:
		log.Printf("[Postgres SQL Value] Unknown type %d, treating as text...\n", dbType)
		return "text"
	}
}
