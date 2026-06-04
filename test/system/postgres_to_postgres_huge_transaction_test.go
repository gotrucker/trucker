package main

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestPostgresToPostgresHugeTransaction(t *testing.T) {
	ctx := context.Background()
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(ctx)
	stop := startTrucker("postgres_to_postgres")

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

	// Insert enough data to exceed logical_decoding_work_mem (64kB in docker-compose),
	// which triggers pgoutput v2 streaming of in-progress transactions.
	sql := "INSERT INTO public.whiskies (name,age,whisky_type_id) VALUES "
	for i := range 8 {
		if i > 0 {
			sql += ","
		}
		sql += "('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',3,1)"
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal("Couldn't start transaction: ", err)
	}
	for range 32 {
		tx.Exec(ctx, sql)
	}
	tx.Commit(ctx)

	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 260 {
			break
		} else if i > 3 {
			t.Error("Expected 260 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(1 * time.Second)
	}

	stop()
}
