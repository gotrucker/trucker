package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestPostgresToPostgresConcurrentTransaction(t *testing.T) {
	ctx := context.Background()
	conn := helpers.PreparePostgresTestDb()
	conn2 := helpers.Connect(helpers.PostgresCfg)
	defer conn.Close(ctx)
	exitChan := startTrucker("postgres_to_postgres")

	// Wait for backfill
	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(ctx, "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 4 {
			break
		} else if i > 10 {
			t.Error("Expected 2 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	newTx := func(c *pgx.Conn) pgx.Tx {
		tx, err := c.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal("Couldn't start transaction: ", err)
		}
		return tx
	}

	insertStuff := func(tx pgx.Tx, text string) {
		for range 2 {
			sql := fmt.Sprintf("INSERT INTO public.whiskies (name,age,whisky_type_id) VALUES ('%s',3,1)", text)
			tx.Exec(ctx, sql)
		}
	}

	txA := newTx(conn)
	txB := newTx(conn2)

	insertStuff(txA, "A1")
	insertStuff(txB, "B1")
	insertStuff(txA, "A2")
	insertStuff(txB, "B2")

	txA.Commit(ctx)
	txB.Commit(ctx)

	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 1000002 {
			break
		} else if i > 120 {
			t.Error("Expected 1000002 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(1 * time.Second)
	}

	close(exitChan)
}
