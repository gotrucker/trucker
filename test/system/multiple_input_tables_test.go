package main

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestMultipleInputTables(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()

	countWhiskies := func() uint64 {
		var cnt proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   "SELECT count(*) cnt FROM trucker.v_whiskies_flat",
			Result: proto.Results{{Name: "cnt", Data: &cnt}},
		}); err != nil {
			t.Error("Failed to query v_whiskies_flat", err)
		}
		return cnt.Row(0)
	}

	// Start trucker with a single table in the truck
	exitChan := cloneProjectAndStart("postgres_to_clickhouse")

	// Wait for backfill
	for i := 0; ; i++ {
		cnt := countWhiskies()
		if cnt == 4 {
			break
		} else if i > 10 {
			t.Error("Expected 4 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Insert some stuff now that we're streaming
	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")
	for i := 0; ; i++ {
		cnt := countWhiskies()
		if cnt == 5 {
			break
		} else if i > 10 {
			t.Error("Expected 5 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Stop trucker
	close(exitChan)

	// Add a row to the whiskies table while trucker is stopped, so we can test catchup.
	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Catchup Whisky', 4, 2)")

	// Start trucker with an extra table in the truck
	exitChan = cloneProjectAndStart("multiple_input_tables")
	defer close(exitChan)

	// Check if backfill runs correctly with the data from the new table
	for i := 0; ; i++ {
		cnt := countWhiskies()
		if cnt == 8 {
			break
		} else if i > 10 {
			t.Error("Expected 8 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Insert some stuff into both input tables and check if it reaches output
	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Last Whisky', 5, 1)")
	pgConn.Exec(context.Background(), "INSERT INTO public.more_whiskies (name, age, whisky_type_id) VALUES ('More Last Whisky', 5, 1)")
	for i := 0; ; i++ {
		cnt := countWhiskies()
		if cnt == 10 {
			break
		} else if i > 10 {
			t.Error("Expected 10 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}
}
