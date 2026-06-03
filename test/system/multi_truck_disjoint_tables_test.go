package main

import (
	"context"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

// TestMultiTruckDisjointTables verifies that two trucks subscribed to different source tables
// each only process changes for their own table — no cross-contamination.
func TestMultiTruckDisjointTables(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	stop := startTrucker("multi_truck_disjoint_tables")
	defer stop()

	countRow := func(table string) uint64 {
		var n uint64
		conn.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n)
		return n
	}

	// Backfill: whiskies (4 rows) → whiskies_flat; more_whiskies (2 rows) → more_whiskies_flat.
	for i := 0; ; i++ {
		a, b := countRow("whiskies_flat"), countRow("more_whiskies_flat")
		if a == 4 && b == 2 {
			break
		} else if i > 20 {
			t.Errorf("expected whiskies_flat=4 more_whiskies_flat=2 after backfill, got %d %d", a, b)
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Insert into whiskies: only whiskies_flat should grow.
	conn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Ardbeg', 10, 4)")
	for i := 0; ; i++ {
		if countRow("whiskies_flat") == 5 {
			break
		} else if i > 20 {
			t.Error("expected whiskies_flat=5 after whiskies insert")
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	if n := countRow("more_whiskies_flat"); n != 2 {
		t.Errorf("more_whiskies_flat should not change on whiskies insert, got %d rows", n)
	}

	// Insert into more_whiskies: only more_whiskies_flat should grow.
	conn.Exec(context.Background(), "INSERT INTO public.more_whiskies (name, age, whisky_type_id) VALUES ('Jameson 18', 18, 3)")
	for i := 0; ; i++ {
		if countRow("more_whiskies_flat") == 3 {
			break
		} else if i > 20 {
			t.Error("expected more_whiskies_flat=3 after more_whiskies insert")
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	if n := countRow("whiskies_flat"); n != 5 {
		t.Errorf("whiskies_flat should not change on more_whiskies insert, got %d rows", n)
	}
}
