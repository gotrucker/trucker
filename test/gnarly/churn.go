//go:build gnarly

package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ChurnResult summarizes mutations driven during a churn run.
type ChurnResult struct {
	Inserted int64
	Updated  int64
	Deleted  int64
}

// startChurn drives INSERT/UPDATE/DELETE traffic against store_sales for the
// given duration and returns a channel that yields ChurnResult when done.
// baseTicket should be set to (max existing ss_ticket_number + 1).
func startChurn(ctx context.Context, conn *pgx.Conn, duration time.Duration, baseTicket int64) <-chan ChurnResult {
	done := make(chan ChurnResult, 1)

	go func() {
		deadline := time.Now().Add(duration)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))

		var inserted, updated, deleted int64
		nextTicket := baseTicket

		// Track tickets inserted in this churn phase for deletes/updates.
		type key struct{ itemSk int32; ticket int64 }
		var inserted_keys []key

		insertTicker := time.NewTicker(5 * time.Millisecond)  // ~200/sec
		updateTicker := time.NewTicker(10 * time.Millisecond) // ~100/sec
		deleteTicker := time.NewTicker(50 * time.Millisecond) // ~20/sec
		defer insertTicker.Stop()
		defer updateTicker.Stop()
		defer deleteTicker.Stop()

		for time.Now().Before(deadline) {
			select {
			case <-insertTicker.C:
				itemSk := int32(rng.Int31n(int32(ItemCount)) + 1)
				ticket := nextTicket
				nextTicket++
				dateSk := int32(dateDimMinSk + int(rng.Int31n(int32(dateDimMaxSk))))
				custSk := int32(rng.Int31n(int32(CustomerCount)) + 1)
				storeSk := int32(rng.Int31n(int32(StoreCount)) + 1)
				netPaid := 10.00 + rng.Float64()*490.00
				_, err := conn.Exec(ctx, `
					INSERT INTO public.store_sales
					(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk,
					 ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number,
					 ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price,
					 ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost,
					 ss_ext_list_price, ss_ext_tax, ss_coupon_amt,
					 ss_net_paid, ss_net_paid_inc_tax, ss_net_profit)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`,
					dateSk, int32(rng.Int31n(86400)),
					itemSk, custSk,
					int32(rng.Int31n(1920800)+1), int32(rng.Int31n(7200)+1),
					int32(rng.Int31n(1000000)+1), storeSk, int32(rng.Int31n(1000)+1), ticket,
					int32(rng.Int31n(100)+1), decimalVal(netPaid*0.5), decimalVal(netPaid*1.1), decimalVal(netPaid),
					decimalVal(rng.Float64()*10), decimalVal(netPaid), decimalVal(netPaid*0.5),
					decimalVal(netPaid*1.1), decimalVal(netPaid*0.08), decimalVal(rng.Float64()*5),
					decimalVal(netPaid), decimalVal(netPaid*1.08), decimalVal(netPaid*0.5),
				)
				if err == nil {
					inserted++
					inserted_keys = append(inserted_keys, key{itemSk, ticket})
				}

			case <-updateTicker.C:
				if len(inserted_keys) == 0 {
					continue
				}
				k := inserted_keys[rng.Intn(len(inserted_keys))]
				newQty := int32(rng.Int31n(200) + 1)
				_, err := conn.Exec(ctx,
					"UPDATE public.store_sales SET ss_quantity = $1 WHERE ss_item_sk = $2 AND ss_ticket_number = $3",
					newQty, k.itemSk, k.ticket,
				)
				if err == nil {
					updated++
				}

			case <-deleteTicker.C:
				if len(inserted_keys) < 10 {
					continue
				}
				// Delete a batch of the oldest 5 inserted rows.
				toDelete := inserted_keys[:5]
				inserted_keys = inserted_keys[5:]

				conds := make([]string, 0, len(toDelete))
				args := make([]any, 0, len(toDelete)*2)
				for j, k := range toDelete {
					conds = append(conds, fmt.Sprintf("(ss_item_sk = $%d AND ss_ticket_number = $%d)", j*2+1, j*2+2))
					args = append(args, k.itemSk, k.ticket)
				}
				sql := "DELETE FROM public.store_sales WHERE " + strings.Join(conds, " OR ")
				_, err := conn.Exec(ctx, sql, args...)
				if err == nil {
					deleted += int64(len(toDelete))
				}

			case <-ctx.Done():
				done <- ChurnResult{inserted, updated, deleted}
				return
			}
		}

		done <- ChurnResult{inserted, updated, deleted}
	}()

	return done
}
