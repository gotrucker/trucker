package main

import (
	"context"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

// TestMultiTruckLSNFloor verifies that the replication slot's confirmed_flush_lsn advances
// when multiple trucks are running — i.e. the min(truckLSNs) gating works end-to-end.
func TestMultiTruckLSNFloor(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	stop := startTrucker("multi_truck_same_table")
	defer stop()

	slotLSN := func() int64 {
		var lsn int64
		conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots").Scan(&lsn)
		return lsn
	}

	// Wait for backfill to complete on both output tables.
	for i := 0; ; i++ {
		var a, b uint64
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat").Scan(&a)
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat_b").Scan(&b)
		if a == 4 && b == 4 {
			break
		} else if i > 20 {
			t.Error("backfill did not complete in time")
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	before := slotLSN()

	// Insert a row and wait for both trucks to write it to their respective output tables.
	conn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Ardbeg', 10, 4)")
	for i := 0; ; i++ {
		var a, b uint64
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat").Scan(&a)
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat_b").Scan(&b)
		if a == 5 && b == 5 {
			break
		} else if i > 20 {
			t.Error("both trucks did not process the insert in time")
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Poll until the keepalive loop confirms the new LSN to PG (fires every 10 seconds).
	deadline := time.Now().Add(15 * time.Second)
	var after int64
	for {
		after = slotLSN()
		if after > before {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("slot confirmed_flush_lsn did not advance: before=%d after=%d", before, after)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}
