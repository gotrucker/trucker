//go:build gnarly

package main

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestTpcdsPgToPg(t *testing.T) {
	pgInConn, pgOutConn := ensureTpcdsLoadedPgOut(t)
	defer pgInConn.Close(context.Background())
	defer pgOutConn.Close(context.Background())

	stop := startTrucker("tpcds_pg_output")
	defer stop()

	t.Run("backfill_volume", func(t *testing.T) {
		type tableCheck struct {
			expr     string
			expected int64
		}
		checks := []tableCheck{
			{"trucker.store_sales_pg WHERE NOT deleted", StoreSalesCount},
			{"trucker.store_pg WHERE NOT deleted", StoreCount},
			{"trucker.date_dim_pg WHERE NOT deleted", DateDimCount},
			{"trucker.customer_pg WHERE NOT deleted", CustomerCount},
			{"trucker.item_pg WHERE NOT deleted", ItemCount},
		}

		for _, check := range checks {
			t.Logf("Waiting for %s to reach %d rows...", check.expr, check.expected)
			deadline := time.Now().Add(10 * time.Minute)
			for {
				var cnt int64
				err := pgOutConn.QueryRow(context.Background(),
					fmt.Sprintf("SELECT count(*) FROM %s", check.expr)).Scan(&cnt)
				if err == nil && cnt == check.expected {
					t.Logf("%s: %d rows confirmed", check.expr, check.expected)
					break
				}
				if time.Now().After(deadline) {
					t.Errorf("%s: expected %d rows after backfill, got %d", check.expr, check.expected, cnt)
					return
				}
				time.Sleep(5 * time.Second)
			}
		}

		var lsn uint64
		if err := pgOutConn.QueryRow(context.Background(),
			"SELECT lsn FROM trucker_current_lsn__pg_input_conn4").Scan(&lsn); err != nil || lsn == 0 {
			t.Error("Expected LSN > 0 after backfill")
		}
	})

	t.Run("wide_row_fidelity", func(t *testing.T) {
		// Compare all 12 store rows: verify wide-row (29-col) decode is lossless.
		type storeRow struct {
			id, storeID, storeName, hours, manager string
			numberEmployees, marketID              int32
			state, country                         string
		}

		pgRows := make(map[string]storeRow)
		rows, err := pgInConn.Query(context.Background(), `
			SELECT s_store_sk::text, s_store_id, s_store_name,
			       s_hours, s_manager, s_number_employees, s_market_id,
			       s_state, s_country
			FROM public.store ORDER BY s_store_sk`)
		if err != nil {
			t.Fatalf("Postgres input store query: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var r storeRow
			if err := rows.Scan(&r.id, &r.storeID, &r.storeName,
				&r.hours, &r.manager, &r.numberEmployees, &r.marketID,
				&r.state, &r.country); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			pgRows[r.id] = r
		}
		rows.Close()
		if len(pgRows) != StoreCount {
			t.Fatalf("Expected %d store rows in Postgres input, got %d", StoreCount, len(pgRows))
		}

		outRows, err := pgOutConn.Query(context.Background(), `
			SELECT id, store_id, store_name, hours, manager,
			       number_employees, market_id, state, country
			FROM trucker.store_pg WHERE NOT deleted ORDER BY id`)
		if err != nil {
			t.Fatalf("Postgres output store_pg query: %v", err)
		}
		defer outRows.Close()

		var count int
		for outRows.Next() {
			var r storeRow
			if err := outRows.Scan(&r.id, &r.storeID, &r.storeName,
				&r.hours, &r.manager, &r.numberEmployees, &r.marketID,
				&r.state, &r.country); err != nil {
				t.Fatalf("Scan output: %v", err)
			}
			count++
			pg, ok := pgRows[r.id]
			if !ok {
				t.Errorf("Store id=%s in output but not in input Postgres", r.id)
				continue
			}
			compare := func(col, pgVal, outVal string) {
				if strings.TrimSpace(pgVal) != strings.TrimSpace(outVal) {
					t.Errorf("store id=%s col=%s: input=%q output=%q", r.id, col, pgVal, outVal)
				}
			}
			compare("store_id", pg.storeID, r.storeID)
			compare("store_name", pg.storeName, r.storeName)
			compare("hours", pg.hours, r.hours)
			compare("manager", pg.manager, r.manager)
			compare("state", pg.state, r.state)
			compare("country", pg.country, r.country)
			if pg.numberEmployees != r.numberEmployees {
				t.Errorf("store id=%s number_employees: input=%d output=%d", r.id, pg.numberEmployees, r.numberEmployees)
			}
			if pg.marketID != r.marketID {
				t.Errorf("store id=%s market_id: input=%d output=%d", r.id, pg.marketID, r.marketID)
			}
		}
		outRows.Close()
		if count != StoreCount {
			t.Fatalf("Expected %d rows in store_pg, got %d", StoreCount, count)
		}
	})

	t.Run("star_join_correctness", func(t *testing.T) {
		// Compute SUM(net_paid) grouped by year × state in input Postgres and output Postgres.
		// Results must match within float64 rounding tolerance.
		type aggKey struct {
			year  int32
			state string
		}
		pgAgg := make(map[aggKey]float64)

		rows, err := pgInConn.Query(context.Background(), `
			SELECT d.d_year, s.s_state, SUM(ss.ss_net_paid::float8)
			FROM public.store_sales ss
			JOIN public.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
			JOIN public.store   s ON ss.ss_store_sk      = s.s_store_sk
			GROUP BY d.d_year, s.s_state
			ORDER BY d.d_year, s.s_state`)
		if err != nil {
			t.Fatalf("Postgres input aggregate query: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var year int32
			var state string
			var sum float64
			if err := rows.Scan(&year, &state, &sum); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			pgAgg[aggKey{year, strings.TrimSpace(state)}] = sum
		}
		rows.Close()

		outRows, err := pgOutConn.Query(context.Background(), `
			SELECT sale_year, store_state, sum(net_paid) AS net_paid_sum
			FROM trucker.store_sales_pg WHERE NOT deleted
			GROUP BY sale_year, store_state
			ORDER BY sale_year, store_state`)
		if err != nil {
			t.Fatalf("Postgres output star join aggregate query: %v", err)
		}
		defer outRows.Close()

		var outCount int
		const sumTolerance = 10.0
		for outRows.Next() {
			var year int32
			var state string
			var sum float64
			if err := outRows.Scan(&year, &state, &sum); err != nil {
				t.Fatalf("Scan output: %v", err)
			}
			outCount++
			key := aggKey{year, strings.TrimSpace(state)}
			pgSum, ok := pgAgg[key]
			if !ok {
				t.Errorf("Output has year=%d state=%s but input Postgres does not", year, state)
				continue
			}
			diff := math.Abs(pgSum - sum)
			if diff > sumTolerance {
				t.Errorf("year=%d state=%s: input sum=%.2f output sum=%.2f (diff=%.2f)",
					year, state, pgSum, sum, diff)
			}
		}
		outRows.Close()
		if outCount != len(pgAgg) {
			t.Errorf("Aggregate row count: input=%d output=%d", len(pgAgg), outCount)
		}
	})

	t.Run("sustained_churn", func(t *testing.T) {
		// Record Postgres input state before churn.
		var pgCountBefore int64
		pgInConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM public.store_sales").Scan(&pgCountBefore)

		// Check initial LSN.
		currentLSN := func() uint64 {
			var lsn uint64
			pgOutConn.QueryRow(context.Background(),
				"SELECT lsn FROM trucker_current_lsn__pg_input_conn4").Scan(&lsn)
			return lsn
		}
		lsnBefore := currentLSN()

		// Drive 30s of churn.
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()

		churnConn := helpers.Connect(helpers.PostgresCfg)
		defer churnConn.Close(context.Background())

		churnDone := startChurn(ctx, churnConn, 30*time.Second, int64(StoreSalesCount)+1)

		// While churn runs, assert LSN keeps advancing (no stall > 15s).
		lastAdvance := time.Now()
		lastLSN := lsnBefore
		stalledErr := false
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
					lsn := currentLSN()
					if lsn > lastLSN {
						lastLSN = lsn
						lastAdvance = time.Now()
					} else if time.Since(lastAdvance) > 15*time.Second && !stalledErr {
						stalledErr = true
					}
				}
			}
		}()

		result := <-churnDone
		cancel()
		<-monitorDone

		if stalledErr {
			t.Error("LSN did not advance for >15s during churn: Trucker appears stalled")
		}

		t.Logf("Churn result: inserted=%d updated=%d deleted=%d", result.Inserted, result.Updated, result.Deleted)

		if result.Inserted < 1000 {
			t.Errorf("Expected at least 1000 inserts during churn, got %d", result.Inserted)
		}

		// Capture Postgres WAL LSN right after churn ends — Trucker must reach this point.
		var churnEndLsnStr string
		pgInConn.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&churnEndLsnStr)
		churnEndLsn := parseLSN(churnEndLsnStr)
		t.Logf("Churn end LSN: %s (%d)", churnEndLsnStr, churnEndLsn)

		// Wait for output LSN tracking table to reach the churn-end LSN.
		lsnDeadline := time.Now().Add(15 * time.Minute)
		for i := 0; time.Now().Before(lsnDeadline); i++ {
			outLsn := currentLSN()
			if outLsn >= churnEndLsn {
				break
			}
			if i%10 == 0 {
				t.Logf("Output LSN: %d (target: %d, gap: %d)", outLsn, churnEndLsn, churnEndLsn-outLsn)
			}
			time.Sleep(3 * time.Second)
		}
		if currentLSN() < churnEndLsn {
			t.Errorf("Output LSN did not reach churn-end LSN %d within 15 minutes", churnEndLsn)
			return
		}

		// LSN caught up — now do the final count/sum assertions once.
		var pgCountAfter int64
		var pgSumAfter float64
		pgInConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM public.store_sales").Scan(&pgCountAfter)
		pgInConn.QueryRow(context.Background(), "SELECT COALESCE(SUM(ss_net_paid::float8), 0) FROM public.store_sales").Scan(&pgSumAfter)
		t.Logf("Postgres input after churn: count=%d sum=%.2f", pgCountAfter, pgSumAfter)

		var outCountAfter int64
		pgOutConn.QueryRow(context.Background(),
			"SELECT count(*) FROM trucker.store_sales_pg WHERE NOT deleted").Scan(&outCountAfter)
		if outCountAfter != pgCountAfter {
			t.Errorf("After churn catchup: input count=%d output count=%d", pgCountAfter, outCountAfter)
			return
		}

		var outSumAfter float64
		pgOutConn.QueryRow(context.Background(),
			"SELECT COALESCE(sum(net_paid), 0) FROM trucker.store_sales_pg WHERE NOT deleted").Scan(&outSumAfter)

		diff := math.Abs(pgSumAfter - outSumAfter)
		const tolerance = 100.0
		if diff > tolerance {
			t.Errorf("Post-churn net_paid sum mismatch: input=%.2f output=%.2f (diff=%.2f)",
				pgSumAfter, outSumAfter, diff)
		} else {
			t.Logf("Post-churn sums match within tolerance: input=%.2f output=%.2f", pgSumAfter, outSumAfter)
		}

		// Verify the churn's deletes landed in the output.
		expectedCount := pgCountBefore + result.Inserted - result.Deleted
		if pgCountAfter != expectedCount {
			t.Errorf("Postgres input row count mismatch after churn: expected %d (base %d + %d inserts - %d deletes), got %d",
				expectedCount, pgCountBefore, result.Inserted, result.Deleted, pgCountAfter)
		}

		if currentLSN() <= lsnBefore {
			t.Error("LSN did not advance at all during churn")
		}
	})
}
