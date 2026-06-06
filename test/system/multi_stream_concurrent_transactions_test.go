package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestMultiStreamConcurrentTransactions(t *testing.T) {
	ctx := context.Background()

	// Prepare both input databases and the output database.
	conn1 := helpers.PreparePostgresTestDb()
	conn2 := helpers.PreparePostgresTestDb2()
	connOut := helpers.PreparePostgresOutputDb()
	defer conn1.Close(ctx)
	defer conn2.Close(ctx)
	defer connOut.Close(ctx)

	// Alter both input whiskies tables to allow explicit ID insertion.
	// GENERATED ALWAYS AS IDENTITY doesn't allow explicit values, so we drop the identity and default.
	conn1.Exec(ctx, "ALTER TABLE public.whiskies ALTER COLUMN id DROP IDENTITY IF EXISTS; ALTER TABLE public.whiskies ALTER COLUMN id DROP DEFAULT;")
	conn2.Exec(ctx, "ALTER TABLE public.whiskies ALTER COLUMN id DROP IDENTITY IF EXISTS; ALTER TABLE public.whiskies ALTER COLUMN id DROP DEFAULT;")

	stop := startTrucker("multi_stream_concurrent_transactions")
	defer stop()

	// Wait for backfill from both streams.
	// Both pg_input and pg_input2 have 4 initial whiskies.
	// Since both write to the same output table, ON CONFLICT causes the second
	// backfill to overwrite the first — final count should be 4.
	for i := 0; ; i++ {
		var cnt uint64
		row := connOut.QueryRow(ctx, "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 4 {
			break
		} else if i > 20 {
			t.Errorf("expected 4 rows in whiskies_flat after backfill (both streams), got %d", cnt)
			return
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Brief pause to ensure both stream goroutines are fully connected
	// and actively listening for WAL changes before we start inserting.
	time.Sleep(1 * time.Second)

	// --- Concurrent inserts across both streams ---
	// Each goroutine opens multiple transactions and interleaves inserts,
	// exercising concurrency within one stream AND across both streams.

	var wg sync.WaitGroup

	// Use explicit IDs to avoid conflicts between streams (both DBs have same sequence)
	insertStreamWithIds := func(c *pgx.Conn, prefix string, count int, idStart int) {
		defer wg.Done()

		for j := 0; j < count; j++ {
			tx, err := c.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				t.Errorf("Couldn't start transaction on %s: %v", prefix, err)
				return
			}

			for k := 0; k < 2; k++ {
				rowId := idStart + j*2 + k
				sql := fmt.Sprintf("INSERT INTO public.whiskies (id, name, age, whisky_type_id) VALUES (%d, '%s_%d_%d', %d, %d)",
					rowId, prefix, j, k, 10+j, (k%5)+1)
				_, err := tx.Exec(ctx, sql)
				if err != nil {
					t.Errorf("Insert failed on %s: %v", prefix, err)
					tx.Rollback(ctx)
					return
				}
			}

			if err := tx.Commit(ctx); err != nil {
				t.Errorf("Commit failed on %s: %v", prefix, err)
			}
		}
	}

	// Run both streams concurrently: 5 transactions per stream, 2 inserts each = 10 rows per stream.
	// Use distinct ID ranges: stream A = 101-110, stream B = 201-210 to avoid conflicts.
	wg.Add(2)
	go insertStreamWithIds(conn1, "A", 5, 101)
	go insertStreamWithIds(conn2, "B", 5, 201)

	// Wait for all inserts to complete.
	wg.Wait()

	// Verify: 4 initial + 10 from stream A + 10 from stream B = 24 rows.
	for i := 0; ; i++ {
		var cnt uint64
		row := connOut.QueryRow(ctx, "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 24 {
			break
		} else if i > 60 {
			t.Errorf("expected 24 rows in whiskies_flat after concurrent inserts, got %d", cnt)
			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Spot-check: verify a row from each stream exists.
	checkRow := func(expectedName string) {
		var name string
		var age int
		row := connOut.QueryRow(ctx, "SELECT name, age FROM whiskies_flat WHERE name = $1", expectedName)
		err := row.Scan(&name, &age)
		if err != nil {
			t.Errorf("expected row %s not found in output: %v", expectedName, err)
			return
		}
		// input.sql computes age delta (new - old = 10 - 0 = 10), output.sql doubles it → 20.
		if age != 20 {
			t.Errorf("expected %s age=20, got %d", expectedName, age)
		}
	}

	checkRow("A_0_0")
	checkRow("B_0_0")
}
