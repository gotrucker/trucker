package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tonyfg/trucker/pkg/db"
)

// ---- helpers ---------------------------------------------------------------

func parserRelation(id uint32, ns, name string, cols ...*pglogrepl.RelationMessageColumn) *pglogrepl.RelationMessageV2 {
	rel := &pglogrepl.RelationMessageV2{
		RelationMessage: pglogrepl.RelationMessage{
			RelationID:   id,
			Namespace:    ns,
			RelationName: name,
			Columns:      cols,
		},
	}
	rel.SetType(pglogrepl.MessageTypeRelation)
	return rel
}

func parserCol(name string) *pglogrepl.RelationMessageColumn {
	return &pglogrepl.RelationMessageColumn{Name: name, DataType: pgtype.TextOID}
}

func parserInsert(relID uint32, vals ...string) *pglogrepl.InsertMessageV2 {
	cols := make([]*pglogrepl.TupleDataColumn, len(vals))
	for i, v := range vals {
		cols[i] = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)}
	}
	m := &pglogrepl.InsertMessageV2{
		InsertMessage: pglogrepl.InsertMessage{
			RelationID: relID,
			Tuple:      &pglogrepl.TupleData{Columns: cols},
		},
	}
	m.SetType(pglogrepl.MessageTypeInsert)
	return m
}

func parserStreamStart(xid uint32, first bool) *pglogrepl.StreamStartMessageV2 {
	firstSeg := uint8(0)
	if first {
		firstSeg = 1
	}
	m := &pglogrepl.StreamStartMessageV2{Xid: xid, FirstSegment: firstSeg}
	m.SetType(pglogrepl.MessageTypeStreamStart)
	return m
}

func parserStreamStop() *pglogrepl.StreamStopMessageV2 {
	m := &pglogrepl.StreamStopMessageV2{}
	m.SetType(pglogrepl.MessageTypeStreamStop)
	return m
}

func parserStreamCommit(xid uint32) *pglogrepl.StreamCommitMessageV2 {
	m := &pglogrepl.StreamCommitMessageV2{Xid: xid}
	m.SetType(pglogrepl.MessageTypeStreamCommit)
	return m
}

func parserStreamAbort(xid uint32) *pglogrepl.StreamAbortMessageV2 {
	m := &pglogrepl.StreamAbortMessageV2{Xid: xid}
	m.SetType(pglogrepl.MessageTypeStreamAbort)
	return m
}

func newTestParser(advance func(string, uint64), subs ...Subscriber) *ReplicationMessageParser {
	done := make(chan struct{})
	return NewReplicationMessageParser(subs, advance, done)
}

func noAdvance(t *testing.T) func(string, uint64) {
	return func(name string, lsn uint64) {
		t.Errorf("unexpected autoAdvance call for %q at lsn %d", name, lsn)
	}
}

func drainChanges(ch chan *db.Changes) []*db.Changes {
	var out []*db.Changes
	for c := range ch {
		out = append(out, c)
	}
	return out
}

// ---- tests -----------------------------------------------------------------

// TestParseStreamMsg covers STREAM_START → inserts → STREAM_STOP → resume → STREAM_COMMIT.
// Verifies a single subscriber receives one transaction containing two Change batches.
func TestParseStreamMsg(t *testing.T) {
	ch := make(chan *db.Transaction, 10)
	var advanced []struct {
		name string
		lsn  uint64
	}
	advance := func(name string, lsn uint64) {
		advanced = append(advanced, struct {
			name string
			lsn  uint64
		}{name, lsn})
	}
	p := newTestParser(advance, Subscriber{Name: "truck1", Tables: map[string]bool{"public.whiskies": true}, Ch: ch})

	rel := parserRelation(1, "public", "whiskies", parserCol("id"), parserCol("name"))

	p.processMsg(rel, 0)
	p.processMsg(parserStreamStart(42, true), 100)
	p.processMsg(parserInsert(1, "1", "Glenfiddich"), 101)
	p.processMsg(parserInsert(1, "2", "Lagavulin"), 102)
	p.processMsg(parserStreamStop(), 103)
	p.processMsg(parserStreamStart(42, false), 104) // resume same xid
	p.processMsg(parserInsert(1, "3", "Hibiki"), 105)
	p.processMsg(parserStreamCommit(42), 200)

	select {
	case txn := <-ch:
		if txn.StreamPosition != 200 {
			t.Errorf("expected StreamPosition=200, got %d", txn.StreamPosition)
		}
		changes := drainChanges(txn.Changes)
		if len(changes) != 2 {
			t.Fatalf("expected 2 changes (one per stream segment), got %d", len(changes))
		}
		// First segment: rows before StreamStop
		rows1 := collectRows(changes[0])
		if len(rows1) != 2 {
			t.Errorf("expected 2 rows in first change, got %d", len(rows1))
		}
		// Second segment: row after StreamStart resume
		rows2 := collectRows(changes[1])
		if len(rows2) != 1 {
			t.Errorf("expected 1 row in second change, got %d", len(rows2))
		}
	default:
		t.Fatal("expected a transaction on Ch, got none")
	}

	if len(advanced) != 0 {
		t.Errorf("expected no autoAdvance calls (truck had rows), got %v", advanced)
	}
}

// TestParseStreamAbort verifies STREAM_ABORT closes in-flight channels without emitting autoAdvance.
func TestParseStreamAbort(t *testing.T) {
	ch := make(chan *db.Transaction, 10)
	p := newTestParser(noAdvance(t), Subscriber{Name: "truck1", Tables: map[string]bool{"public.whiskies": true}, Ch: ch})

	rel := parserRelation(1, "public", "whiskies", parserCol("id"))
	p.processMsg(rel, 0)
	p.processMsg(parserStreamStart(7, true), 100)
	p.processMsg(parserInsert(1, "1"), 101)
	p.processMsg(parserStreamAbort(7), 102)

	select {
	case txn := <-ch:
		// Changes channel must be closed (abort flushed it)
		changes := drainChanges(txn.Changes)
		if len(changes) != 1 {
			t.Fatalf("expected 1 change before abort, got %d", len(changes))
		}
		rows := collectRows(changes[0])
		if len(rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(rows))
		}
	default:
		t.Fatal("expected a transaction on Ch (rows arrived before abort)")
	}
}

// TestParserMultiSubscriber_SameTable verifies both subscribers on the same table receive all rows independently.
func TestParserMultiSubscriber_SameTable(t *testing.T) {
	chA := make(chan *db.Transaction, 10)
	chB := make(chan *db.Transaction, 10)
	p := newTestParser(noAdvance(t),
		Subscriber{Name: "truckA", Tables: map[string]bool{"public.x": true}, Ch: chA},
		Subscriber{Name: "truckB", Tables: map[string]bool{"public.x": true}, Ch: chB},
	)

	rel := parserRelation(1, "public", "x", parserCol("v"))
	p.processMsg(rel, 0)
	p.processMsg(parserStreamStart(1, true), 10)
	p.processMsg(parserInsert(1, "hello"), 11)
	p.processMsg(parserInsert(1, "world"), 12)
	p.processMsg(parserStreamCommit(1), 50)

	for label, ch := range map[string]chan *db.Transaction{"truckA": chA, "truckB": chB} {
		select {
		case txn := <-ch:
			changes := drainChanges(txn.Changes)
			if len(changes) != 1 {
				t.Errorf("%s: expected 1 change, got %d", label, len(changes))
				continue
			}
			rows := collectRows(changes[0])
			if len(rows) != 2 {
				t.Errorf("%s: expected 2 rows, got %d", label, len(rows))
			}
		default:
			t.Errorf("%s: expected transaction on Ch, got none", label)
		}
	}
}

// TestParserMultiSubscriber_DisjointTables verifies that each subscriber only sees rows from its own table.
func TestParserMultiSubscriber_DisjointTables(t *testing.T) {
	chA := make(chan *db.Transaction, 10)
	chB := make(chan *db.Transaction, 10)
	p := newTestParser(noAdvance(t),
		Subscriber{Name: "truckA", Tables: map[string]bool{"public.whiskies": true}, Ch: chA},
		Subscriber{Name: "truckB", Tables: map[string]bool{"public.spirits": true}, Ch: chB},
	)

	relW := parserRelation(1, "public", "whiskies", parserCol("name"))
	relS := parserRelation(2, "public", "spirits", parserCol("name"))
	p.processMsg(relW, 0)
	p.processMsg(relS, 0)
	p.processMsg(parserStreamStart(5, true), 10)
	p.processMsg(parserInsert(1, "Glenfiddich"), 11) // whiskies → truckA only
	p.processMsg(parserInsert(2, "Rum"), 12)         // spirits → truckB only
	p.processMsg(parserStreamCommit(5), 50)

	select {
	case txnA := <-chA:
		changesA := drainChanges(txnA.Changes)
		if len(changesA) != 1 {
			t.Fatalf("truckA: expected 1 change, got %d", len(changesA))
		}
		if changesA[0].Table != "public.whiskies" {
			t.Errorf("truckA: expected whiskies change, got %q", changesA[0].Table)
		}
		if rows := collectRows(changesA[0]); len(rows) != 1 {
			t.Errorf("truckA: expected 1 row, got %d", len(rows))
		}
	default:
		t.Fatal("expected transaction on truckA Ch")
	}

	select {
	case txnB := <-chB:
		changesB := drainChanges(txnB.Changes)
		if len(changesB) != 1 {
			t.Fatalf("truckB: expected 1 change, got %d", len(changesB))
		}
		if changesB[0].Table != "public.spirits" {
			t.Errorf("truckB: expected spirits change, got %q", changesB[0].Table)
		}
		if rows := collectRows(changesB[0]); len(rows) != 1 {
			t.Errorf("truckB: expected 1 row, got %d", len(rows))
		}
	default:
		t.Fatal("expected transaction on truckB Ch")
	}
}

// TestParserAutoAdvanceOnIdleSubscriber verifies that a subscriber with no rows in a committed xid
// receives an autoAdvance call instead of a transaction.
func TestParserAutoAdvanceOnIdleSubscriber(t *testing.T) {
	chA := make(chan *db.Transaction, 10)
	chB := make(chan *db.Transaction, 10)

	var advancedFor string
	var advancedLSN uint64
	advance := func(name string, lsn uint64) {
		advancedFor = name
		advancedLSN = lsn
	}
	p := newTestParser(advance,
		Subscriber{Name: "truckA", Tables: map[string]bool{"public.whiskies": true}, Ch: chA},
		Subscriber{Name: "truckB", Tables: map[string]bool{"public.spirits": true}, Ch: chB},
	)

	rel := parserRelation(1, "public", "whiskies", parserCol("name"))
	p.processMsg(rel, 0)
	p.processMsg(parserStreamStart(9, true), 10)
	p.processMsg(parserInsert(1, "Glenfiddich"), 11) // only whiskies — truckB has no rows
	p.processMsg(parserStreamCommit(9), 99)

	select {
	case <-chA:
	default:
		t.Fatal("expected transaction on truckA (it had rows)")
	}
	select {
	case <-chB:
		t.Fatal("truckB should not receive a transaction (no rows for it)")
	default:
	}

	if advancedFor != "truckB" {
		t.Errorf("expected autoAdvance for truckB, got %q", advancedFor)
	}
	if advancedLSN != 99 {
		t.Errorf("expected autoAdvance lsn=99, got %d", advancedLSN)
	}
}

// TestParserFlushAllOnEarlyExit verifies that flushAll closes in-flight channels so consumers unblock.
func TestParserFlushAllOnEarlyExit(t *testing.T) {
	ch := make(chan *db.Transaction, 10)
	p := newTestParser(noAdvance(t), Subscriber{Name: "truck1", Tables: map[string]bool{"public.x": true}, Ch: ch})

	rel := parserRelation(1, "public", "x", parserCol("v"))
	p.processMsg(rel, 0)
	p.processMsg(parserStreamStart(3, true), 50)
	p.processMsg(parserInsert(1, "partial"), 51) // creates txn and open Changes

	p.flushAll() // simulate early exit before StreamCommit

	select {
	case txn := <-ch:
		// drainChanges terminates only if txn.Changes was closed by flushAll
		changes := drainChanges(txn.Changes)
		if len(changes) != 1 {
			t.Fatalf("expected 1 partial change, got %d", len(changes))
		}
		// collectRows terminates only if Rows was closed by flushAll
		rows := collectRows(changes[0])
		if len(rows) != 1 {
			t.Errorf("expected 1 partial row, got %d", len(rows))
		}
	default:
		t.Fatal("expected a transaction on Ch (was created before flushAll)")
	}
}
