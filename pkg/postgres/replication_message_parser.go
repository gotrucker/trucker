package postgres

import (
	// "bytes"
	// "encoding/json"
	"fmt"
	// "iter"
	"log"
	// "slices"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jackc/pglogrepl"
	"github.com/tonyfg/trucker/pkg/db"
)

type ReplicationMessageParser struct {
	inStream     bool
	currentTable string
	transaction  *db.Transaction
	changes      *db.Changes
	typeMap      *pgtype.Map
	relations    map[uint32]*pglogrepl.RelationMessageV2
}

func NewReplicationMessageParser() *ReplicationMessageParser {
	return &ReplicationMessageParser{
		inStream:  false,
		typeMap:   pgtype.NewMap(),
		relations: map[uint32]*pglogrepl.RelationMessageV2{},
	}
}

func (p *ReplicationMessageParser) parseReplicationMsg(walData []byte, streamPosition uint64) *db.Transaction {
	logicalMsg, err := pglogrepl.ParseV2(walData, p.inStream)
	if err != nil {
		log.Fatalf("Error parsing logical replication message: %s", err)
	}
	log.Printf("Logical replication message: %T\n", logicalMsg)

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		log.Printf("RelationMessageV2: %s.%s\n", logicalMsg.Namespace, logicalMsg.RelationName)
		p.relations[logicalMsg.RelationID] = logicalMsg
		p.currentTable = fmt.Sprintf("%s.%s", logicalMsg.Namespace, logicalMsg.RelationName)

	case *pglogrepl.BeginMessage, *pglogrepl.StreamStartMessageV2:
		if p.inStream {
			log.Fatal("Warning: received BEGIN/STREAM_START while already in a stream!")
		}
		p.inStream = true

		if p.transaction == nil {
			p.transaction = &db.Transaction{
				StreamPosition: streamPosition,
				Changes:        make(chan *db.Changes, 128),
			}
			return p.transaction
		}

	case *pglogrepl.CommitMessage, *pglogrepl.StreamCommitMessageV2:
		if p.changes != nil {
			close(p.changes.Rows)
			p.transaction.Changes <- p.changes
			p.changes = nil
		}

		close(p.transaction.Changes)
		p.transaction = nil
		p.inStream = false

	case *pglogrepl.InsertMessageV2:
		rel, ok := p.relations[logicalMsg.RelationID]
		if !ok {
			log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
		}
		values := map[string]any{}
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
		rel, ok := p.relations[logicalMsg.RelationID]
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
		// logicalMsg.OldTuple.Columns
		log.Printf("delete for xid %d\n", logicalMsg.Xid)
	// ...
	case *pglogrepl.TruncateMessageV2:
		log.Printf("truncate for xid %d\n", logicalMsg.Xid)
	// ...

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false
		log.Printf("Stream stop message")
	case *pglogrepl.StreamAbortMessageV2:
		log.Printf("Stream abort message: xid %d", logicalMsg.Xid)
	}

	return nil, true // CONA apaga isto
}

func (p *ReplicationMessageParser) parseColumn(idx int, col *pglogrepl.TupleDataColumn) {

}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
