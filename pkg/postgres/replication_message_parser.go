package postgres

import (
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tonyfg/trucker/pkg/db"
)

type subscriberCtx struct {
	sub  *Subscriber
	txn  *db.Transaction
	open *db.Changes
	done bool // true once the subscriber's KillChan or rcDone fires
}

type ReplicationMessageParser struct {
	inStream    bool
	currentXid  uint32
	streamPos   uint64
	subs        []*subscriberCtx
	tableIndex  map[string][]*subscriberCtx
	typeMap     *pgtype.Map
	relations   map[uint32]*pglogrepl.RelationMessageV2
	autoAdvance func(name string, lsn uint64)
	rcDone      <-chan struct{}
}

func NewReplicationMessageParser(subs []Subscriber, autoAdvance func(string, uint64), rcDone <-chan struct{}) *ReplicationMessageParser {
	ctxs := make([]*subscriberCtx, len(subs))
	tableIndex := make(map[string][]*subscriberCtx)
	for i := range subs {
		sc := &subscriberCtx{sub: &subs[i]}
		ctxs[i] = sc
		for table := range subs[i].Tables {
			tableIndex[table] = append(tableIndex[table], sc)
		}
	}
	return &ReplicationMessageParser{
		subs:        ctxs,
		tableIndex:  tableIndex,
		typeMap:     pgtype.NewMap(),
		relations:   make(map[uint32]*pglogrepl.RelationMessageV2),
		autoAdvance: autoAdvance,
		rcDone:      rcDone,
	}
}

func (p *ReplicationMessageParser) parseReplicationMsg(walData []byte, streamPosition uint64) {
	logicalMsg, err := pglogrepl.ParseV2(walData, p.inStream)
	if err != nil {
		log.Fatalf("Error parsing logical replication message: %s", err)
	}
	p.processMsg(logicalMsg, streamPosition)
}

func (p *ReplicationMessageParser) processMsg(msg pglogrepl.Message, streamPosition uint64) {
	switch logicalMsg := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		p.relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		p.streamPos = streamPosition

	case *pglogrepl.StreamStartMessageV2:
		if p.inStream {
			// streaming='true' serializes xids; a different xid here is unsupported
			if logicalMsg.Xid != p.currentXid {
				log.Fatalf("[parser] STREAM_START for xid %d while streaming xid %d — concurrent streaming requires streaming='parallel' (not supported)", logicalMsg.Xid, p.currentXid)
			}
			// same xid resuming after StreamStop — state is intact
		} else {
			p.inStream = true
			p.currentXid = logicalMsg.Xid
			p.streamPos = streamPosition
		}

	case *pglogrepl.InsertMessageV2:
		rel := p.requireRelation(logicalMsg.RelationID)
		tableName := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
		cols := changesetCols(p.columnsFromRelation(rel))
		newRow := p.decodeTuple(logicalMsg.Tuple, rel)
		oldRow := make([]any, len(rel.Columns))
		p.routeRow(tableName, db.Insert, cols, append(newRow, oldRow...))

	case *pglogrepl.UpdateMessageV2:
		rel := p.requireRelation(logicalMsg.RelationID)
		tableName := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
		cols := changesetCols(p.columnsFromRelation(rel))
		newRow := p.decodeTuple(logicalMsg.NewTuple, rel)
		oldRow := p.decodeTuple(logicalMsg.OldTuple, rel)
		p.routeRow(tableName, db.Update, cols, append(newRow, oldRow...))

	case *pglogrepl.DeleteMessageV2:
		rel := p.requireRelation(logicalMsg.RelationID)
		tableName := fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
		cols := changesetCols(p.columnsFromRelation(rel))
		newRow := make([]any, len(rel.Columns))
		oldRow := p.decodeTuple(logicalMsg.OldTuple, rel)
		p.routeRow(tableName, db.Delete, cols, append(newRow, oldRow...))

	case *pglogrepl.CommitMessage:
		p.commitAll(streamPosition)

	case *pglogrepl.StreamCommitMessageV2:
		p.commitAll(streamPosition)
		p.inStream = false

	case *pglogrepl.StreamStopMessageV2:
		p.inStream = false
		for _, sc := range p.subs {
			if sc.open != nil {
				close(sc.open.Rows)
				sc.open = nil
			}
		}

	case *pglogrepl.StreamAbortMessageV2:
		for _, sc := range p.subs {
			if sc.open != nil {
				close(sc.open.Rows)
				sc.open = nil
			}
			if sc.txn != nil {
				close(sc.txn.Changes)
				sc.txn = nil
			}
		}
		p.inStream = false
		p.currentXid = 0

	case *pglogrepl.TruncateMessageV2:
		log.Printf("[parser] TRUNCATE not yet supported (xid %d)\n", logicalMsg.Xid)
	}
}

func (p *ReplicationMessageParser) commitAll(commitLSN uint64) {
	for _, sc := range p.subs {
		if sc.open != nil {
			close(sc.open.Rows)
			sc.open = nil
		}
		if sc.txn != nil {
			close(sc.txn.Changes)
			sc.txn = nil
		} else {
			// subscriber had no rows in this xid — advance its ack without involving the truck
			p.autoAdvance(sc.sub.Name, commitLSN)
		}
	}
	p.currentXid = 0
	p.streamPos = 0
}

func (p *ReplicationMessageParser) routeRow(tableName string, op uint8, cols []db.Column, row []any) {
	targets := p.tableIndex[tableName]
	if len(targets) == 0 {
		return
	}
	for _, sc := range targets {
		if sc.done {
			continue
		}
		if !p.ensureTxn(sc) {
			continue
		}
		p.ensureChanges(sc, tableName, op, cols)
		sc.open.Rows <- [][]any{row}
	}
}

// ensureTxn lazily creates a transaction for the subscriber and sends it on the subscriber's channel.
// Returns false if the subscriber's done channel fired (truck shutting down).
func (p *ReplicationMessageParser) ensureTxn(sc *subscriberCtx) bool {
	if sc.txn != nil {
		return true
	}
	txn := &db.Transaction{
		StreamPosition: p.streamPos,
		Changes:        make(chan *db.Changes, 128),
	}
	select {
	case sc.sub.Ch <- *txn:
		sc.txn = txn
		return true
	case <-sc.sub.Done: // nil channel: never selected — safe for subscribers without a Done channel
		sc.done = true
		return false
	case <-p.rcDone:
		sc.done = true
		return false
	}
}

func (p *ReplicationMessageParser) ensureChanges(sc *subscriberCtx, tableName string, op uint8, cols []db.Column) {
	if sc.open != nil && sc.open.Table == tableName && sc.open.Operation == op {
		return
	}
	if sc.open != nil {
		close(sc.open.Rows)
	}
	sc.open = &db.Changes{
		Table:     tableName,
		Operation: op,
		Columns:   cols,
		Rows:      make(chan [][]any, 3),
	}
	sc.txn.Changes <- sc.open
}

// flushAll closes all in-flight Changes and Transaction channels so consumers unblock.
// Does NOT close subscriber (Ch) channels — call closeSubscribers for permanent shutdown.
func (p *ReplicationMessageParser) flushAll() {
	for _, sc := range p.subs {
		if sc.open != nil {
			close(sc.open.Rows)
			sc.open = nil
		}
		if sc.txn != nil {
			close(sc.txn.Changes)
			sc.txn = nil
		}
	}
}

// closeSubscribers closes each subscriber's transaction channel so truck goroutines exit cleanly.
// Call only on permanent RC shutdown (not between catchup and stream phases).
func (p *ReplicationMessageParser) closeSubscribers() {
	for _, sc := range p.subs {
		if !sc.done {
			close(sc.sub.Ch)
		}
	}
}

func (p *ReplicationMessageParser) requireRelation(id uint32) *pglogrepl.RelationMessageV2 {
	rel, ok := p.relations[id]
	if !ok {
		log.Fatalf("unknown relation ID %d", id)
	}
	return rel
}

func (p *ReplicationMessageParser) columnsFromRelation(rel *pglogrepl.RelationMessageV2) []db.Column {
	cols := make([]db.Column, len(rel.Columns))
	for i, col := range rel.Columns {
		cols[i] = db.Column{
			Name: col.Name,
			Type: oidToDbType(col.DataType),
		}
	}
	return cols
}

func (p *ReplicationMessageParser) decodeTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) []any {
	row := make([]any, len(rel.Columns))
	if tuple == nil {
		return row
	}
	for idx, col := range tuple.Columns {
		switch col.DataType {
		case 'n':
			row[idx] = nil
		case 'u':
			row[idx] = nil
		case 't':
			val, err := decodeTextColumnData(p.typeMap, col.Data, rel.Columns[idx].DataType)
			if err != nil {
				log.Fatalln("error decoding column data:", err)
			}
			row[idx] = val
		}
	}
	return row
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
