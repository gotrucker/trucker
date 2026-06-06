//go:build gnarly

package main

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestTpcdsGnarly(t *testing.T) {
	pgConn, chConn := ensureTpcdsLoaded(t)
	defer pgConn.Close(context.Background())
	defer chConn.Close()

	stop := startTrucker("tpcds")
	defer stop()

	t.Run("backfill_volume", func(t *testing.T) {
		type tableCheck struct {
			view     string
			expected int64
		}
		checks := []tableCheck{
			{"trucker.v_store_sales_flat", StoreSalesCount},
			{"trucker.v_store_ch", StoreCount},
			{"trucker.v_date_dim_ch", DateDimCount},
			{"trucker.v_customer_ch", CustomerCount},
			{"trucker.v_item_ch", ItemCount},
		}

		for _, check := range checks {
			t.Logf("Waiting for %s to reach %d rows...", check.view, check.expected)
			deadline := time.Now().Add(10 * time.Minute)
			for {
				var cnt proto.ColUInt64
				if err := chConn.Do(context.Background(), ch.Query{
					Body:   fmt.Sprintf("SELECT count() cnt FROM %s", check.view),
					Result: proto.Results{{Name: "cnt", Data: &cnt}},
				}); err != nil {
					t.Logf("ClickHouse query error for %s: %v", check.view, err)
				} else if cnt.Rows() > 0 && int64(cnt.Row(0)) == check.expected {
					t.Logf("%s: %d rows confirmed", check.view, check.expected)
					break
				}
				if time.Now().After(deadline) {
					var got int64
					if cnt.Rows() > 0 {
						got = int64(cnt.Row(0))
					}
					t.Errorf("%s: expected %d rows after backfill, got %d", check.view, check.expected, got)
					return
				}
				time.Sleep(5 * time.Second)
			}
		}

		var lsn proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   `SELECT lsn FROM "trucker"."trucker_current_lsn__pg_input_conn3" FINAL`,
			Result: proto.Results{{Name: "lsn", Data: &lsn}},
		}); err != nil || lsn.Rows() == 0 || lsn.Row(0) == 0 {
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
		rows, err := pgConn.Query(context.Background(), `
			SELECT s_store_sk::text, s_store_id, s_store_name,
			       s_hours, s_manager, s_number_employees, s_market_id,
			       s_state, s_country
			FROM public.store ORDER BY s_store_sk`)
		if err != nil {
			t.Fatalf("Postgres store query: %v", err)
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
			t.Fatalf("Expected %d store rows in Postgres, got %d", StoreCount, len(pgRows))
		}

		var chID, chStoreID, chStoreName, chHours, chManager, chState, chCountry proto.ColStr
		var chNumEmp, chMarketID proto.ColInt32
		if err := chConn.Do(context.Background(), ch.Query{
			Body: `SELECT id, store_id, store_name, hours, manager,
			              number_employees, market_id, state, country
			       FROM trucker.v_store_ch ORDER BY id`,
			Result: proto.Results{
				{Name: "id", Data: &chID},
				{Name: "store_id", Data: &chStoreID},
				{Name: "store_name", Data: &chStoreName},
				{Name: "hours", Data: &chHours},
				{Name: "manager", Data: &chManager},
				{Name: "number_employees", Data: &chNumEmp},
				{Name: "market_id", Data: &chMarketID},
				{Name: "state", Data: &chState},
				{Name: "country", Data: &chCountry},
			},
		}); err != nil {
			t.Fatalf("ClickHouse v_store_ch query: %v", err)
		}

		if chID.Rows() != StoreCount {
			t.Fatalf("Expected %d rows in v_store_ch, got %d", StoreCount, chID.Rows())
		}

		for i := 0; i < chID.Rows(); i++ {
			id := chID.Row(i)
			pg, ok := pgRows[id]
			if !ok {
				t.Errorf("Store id=%s in ClickHouse but not Postgres", id)
				continue
			}
			compare := func(col, pgVal, chVal string) {
				if strings.TrimSpace(pgVal) != strings.TrimSpace(chVal) {
					t.Errorf("store id=%s col=%s: Postgres=%q ClickHouse=%q", id, col, pgVal, chVal)
				}
			}
			compare("store_id", pg.storeID, chStoreID.Row(i))
			compare("store_name", pg.storeName, chStoreName.Row(i))
			compare("hours", pg.hours, chHours.Row(i))
			compare("manager", pg.manager, chManager.Row(i))
			compare("state", pg.state, chState.Row(i))
			compare("country", pg.country, chCountry.Row(i))
			if pg.numberEmployees != chNumEmp.Row(i) {
				t.Errorf("store id=%s number_employees: Postgres=%d ClickHouse=%d", id, pg.numberEmployees, chNumEmp.Row(i))
			}
			if pg.marketID != chMarketID.Row(i) {
				t.Errorf("store id=%s market_id: Postgres=%d ClickHouse=%d", id, pg.marketID, chMarketID.Row(i))
			}
		}
	})

	t.Run("star_join_correctness", func(t *testing.T) {
		// Compute SUM(net_paid) grouped by year × state in Postgres and ClickHouse.
		// Results must match within float64 rounding tolerance.
		type aggKey struct {
			year  int32
			state string
		}
		pgAgg := make(map[aggKey]float64)

		rows, err := pgConn.Query(context.Background(), `
			SELECT d.d_year, s.s_state, SUM(ss.ss_net_paid::float8)
			FROM public.store_sales ss
			JOIN public.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
			JOIN public.store   s ON ss.ss_store_sk      = s.s_store_sk
			GROUP BY d.d_year, s.s_state
			ORDER BY d.d_year, s.s_state`)
		if err != nil {
			t.Fatalf("Postgres aggregate query: %v", err)
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

		var chYear proto.ColInt32
		var chState proto.ColStr
		var chSum proto.ColFloat64
		if err := chConn.Do(context.Background(), ch.Query{
			Body: `SELECT sale_year, store_state, sum(net_paid) AS net_paid_sum
			       FROM trucker.v_store_sales_flat
			       GROUP BY sale_year, store_state
			       ORDER BY sale_year, store_state`,
			Result: proto.Results{
				{Name: "sale_year", Data: &chYear},
				{Name: "store_state", Data: &chState},
				{Name: "net_paid_sum", Data: &chSum},
			},
		}); err != nil {
			t.Fatalf("ClickHouse star join aggregate query: %v", err)
		}

		if chYear.Rows() != len(pgAgg) {
			t.Errorf("Aggregate row count: Postgres=%d ClickHouse=%d", len(pgAgg), chYear.Rows())
		}

		const sumTolerance = 10.0
		for i := 0; i < chYear.Rows(); i++ {
			key := aggKey{chYear.Row(i), strings.TrimSpace(chState.Row(i))}
			pgSum, ok := pgAgg[key]
			if !ok {
				t.Errorf("ClickHouse has year=%d state=%s but Postgres does not", key.year, key.state)
				continue
			}
			diff := math.Abs(pgSum - chSum.Row(i))
			if diff > sumTolerance {
				t.Errorf("year=%d state=%s: Postgres sum=%.2f ClickHouse sum=%.2f (diff=%.2f)",
					key.year, key.state, pgSum, chSum.Row(i), diff)
			}
		}
	})

	t.Run("sustained_churn", func(t *testing.T) {
		// Record Postgres state before churn.
		var pgCountBefore int64
		pgConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM public.store_sales").Scan(&pgCountBefore)

		// Check initial LSN.
		currentLSN := func() uint64 {
			var lsn proto.ColUInt64
			chConn.Do(context.Background(), ch.Query{
				Body:   `SELECT lsn FROM "trucker"."trucker_current_lsn__pg_input_conn3" FINAL`,
				Result: proto.Results{{Name: "lsn", Data: &lsn}},
			})
			if lsn.Rows() > 0 {
				return lsn.Row(0)
			}
			return 0
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
		pgConn.QueryRow(context.Background(), "SELECT pg_current_wal_lsn()::text").Scan(&churnEndLsnStr)
		churnEndLsn := parseLSN(churnEndLsnStr)
		t.Logf("Churn end LSN: %s (%d)", churnEndLsnStr, churnEndLsn)

		// Wait for ClickHouse LSN tracking table to reach the churn-end LSN.
		// This is an O(1) query and far cheaper than aggregating 3M+ rows.
		lsnDeadline := time.Now().Add(15 * time.Minute)
		for i := 0; time.Now().Before(lsnDeadline); i++ {
			chLsn := currentLSN()
			if chLsn >= churnEndLsn {
				break
			}
			if i%10 == 0 {
				t.Logf("ClickHouse LSN: %d (target: %d, gap: %d)", chLsn, churnEndLsn, churnEndLsn-chLsn)
			}
			time.Sleep(3 * time.Second)
		}
		if currentLSN() < churnEndLsn {
			t.Errorf("ClickHouse LSN did not reach churn-end LSN %d within 15 minutes", churnEndLsn)
			return
		}

		// LSN caught up — now do the final count/sum assertions once.
		var pgCountAfter int64
		var pgSumAfter float64
		pgConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM public.store_sales").Scan(&pgCountAfter)
		pgConn.QueryRow(context.Background(), "SELECT COALESCE(SUM(ss_net_paid::float8), 0) FROM public.store_sales").Scan(&pgSumAfter)
		t.Logf("Postgres after churn: count=%d sum=%.2f", pgCountAfter, pgSumAfter)

		var chCountAfter proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   "SELECT count() cnt FROM trucker.v_store_sales_flat",
			Result: proto.Results{{Name: "cnt", Data: &chCountAfter}},
		}); err != nil {
			t.Fatalf("ClickHouse post-churn count query failed: %v", err)
		}
		if chCountAfter.Rows() == 0 || int64(chCountAfter.Row(0)) != pgCountAfter {
			var got int64
			if chCountAfter.Rows() > 0 {
				got = int64(chCountAfter.Row(0))
			}
			t.Errorf("After churn catchup: Postgres count=%d ClickHouse count=%d", pgCountAfter, got)
			return
		}

		var chSumAfter proto.ColFloat64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   "SELECT sum(net_paid) s FROM trucker.v_store_sales_flat",
			Result: proto.Results{{Name: "s", Data: &chSumAfter}},
		}); err != nil || chSumAfter.Rows() == 0 {
			t.Fatalf("ClickHouse post-churn sum query failed: %v", err)
		}

		diff := math.Abs(pgSumAfter - chSumAfter.Row(0))
		const tolerance = 100.0
		if diff > tolerance {
			t.Errorf("Post-churn net_paid sum mismatch: Postgres=%.2f ClickHouse=%.2f (diff=%.2f)",
				pgSumAfter, chSumAfter.Row(0), diff)
		} else {
			t.Logf("Post-churn sums match within tolerance: Postgres=%.2f ClickHouse=%.2f", pgSumAfter, chSumAfter.Row(0))
		}

		// Verify the churn's deletes landed in ClickHouse.
		expectedCount := pgCountBefore + result.Inserted - result.Deleted
		if pgCountAfter != expectedCount {
			t.Errorf("Postgres row count mismatch after churn: expected %d (base %d + %d inserts - %d deletes), got %d",
				expectedCount, pgCountBefore, result.Inserted, result.Deleted, pgCountAfter)
		}

		if currentLSN() <= lsnBefore {
			t.Error("LSN did not advance at all during churn")
		}
	})
}

// pgQueryFloat64 runs a single-row/column float64 query against Postgres.
func pgQueryFloat64(conn *pgx.Conn, sql string) float64 {
	var v float64
	conn.QueryRow(context.Background(), sql).Scan(&v)
	return v
}

// parseLSN converts a Postgres LSN string ("A/B") to a uint64.
func parseLSN(s string) uint64 {
	var hi, lo uint64
	fmt.Sscanf(s, "%X/%X", &hi, &lo)
	return (hi << 32) | lo
}
