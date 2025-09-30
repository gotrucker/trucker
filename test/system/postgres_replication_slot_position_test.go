package main

import (
	"context"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestPostgresReplicationSlotPosition(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	exitChan := startTrucker("postgres_to_postgres")
	defer close(exitChan)

	getCurrentLsn := func() int64 {
		var lsn int64
		row := conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn - '0/0' FROM pg_replication_slots")
		row.Scan(&lsn)
		return lsn
	}
	curPos := getCurrentLsn()

	// Test updated LSN after insert
	conn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")
	for i := 0; ; i++ {
		newCurPos := getCurrentLsn()

		if newCurPos > curPos {
			curPos = newCurPos
			break
		} else if i > 100 {
			t.Error("Expected LSN to have increased after inserts but it didn't")
			break
		}

		time.Sleep(300 * time.Millisecond)
	}
}
