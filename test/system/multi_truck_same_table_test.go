package main

import (
	"context"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

// TestMultiTruckSameTable verifies that two trucks subscribed to the same source table
// each independently receive and process every row, writing to distinct output tables.
func TestMultiTruckSameTable(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	stop := startTrucker("multi_truck_same_table")
	defer stop()

	countBoth := func() (uint64, uint64) {
		var a, b uint64
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat").Scan(&a)
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat_b").Scan(&b)
		return a, b
	}

	// Both output tables must receive the full backfill.
	for i := 0; ; i++ {
		a, b := countBoth()
		if a == 4 && b == 4 {
			break
		} else if i > 20 {
			t.Errorf("expected whiskies_flat=4 whiskies_flat_b=4 after backfill, got %d %d", a, b)
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Insert a new whisky; both trucks must propagate it.
	conn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Ardbeg', 10, 4)")
	for i := 0; ; i++ {
		a, b := countBoth()
		if a == 5 && b == 5 {
			break
		} else if i > 20 {
			t.Errorf("expected whiskies_flat=5 whiskies_flat_b=5 after insert, got %d %d", a, b)
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Update and verify both tables reflect the change.
	conn.Exec(context.Background(), "UPDATE public.whiskies SET age = 15 WHERE name = 'Ardbeg'")
	check := func(table string) {
		for i := 0; ; i++ {
			var age int
			conn.QueryRow(context.Background(), "SELECT age FROM "+table+" WHERE name = 'Ardbeg'").Scan(&age)
			// output.sql multiplies age by 2; update delta = 15 - 10 = 5, so stored age = (5)*2 = 10
			// (the input.sql computes COALESCE(new,0) - COALESCE(old,0) then output doubles it)
			if age == 10 {
				break
			} else if i > 20 {
				t.Errorf("%s: expected Ardbeg age=10 after update, got %d", table, age)
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	check("whiskies_flat")
	check("whiskies_flat_b")
}
