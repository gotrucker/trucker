package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestPostgresToClickhouse(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()

	countWhiskies := func(criteria string) uint64 {
		var cnt proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   fmt.Sprintf("SELECT count(*) cnt FROM trucker.v_whiskies_flat %s", criteria),
			Result: proto.Results{{Name: "cnt", Data: &cnt}},
		}); err != nil {
			t.Error("Failed to query v_whiskies_flat", err)
		}
		return cnt.Row(0)
	}

	currentPosition := func() uint64 {
		var lsn proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   "SELECT lsn lsn FROM trucker.trucker_current_lsn__pg_input_conn2 FINAL",
			Result: proto.Results{{Name: "lsn", Data: &lsn}},
		}); err != nil {
			t.Error("Failed to query trucker_current_lsn__pg_input_conn2", err)
		}
		return lsn.Row(0)
	}

	exitChan := startTrucker("postgres_to_clickhouse")
	defer close(exitChan)

	// Test backfill
	for i := 0; ; i++ {
		cnt := countWhiskies("")
		if cnt == 4 {
			break
		} else if i > 10 {
			t.Error("Expected 4 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	curPos := currentPosition()
	if currentPosition() == 0 {
		t.Error("Expected LSN to be greater than 0 after backfill")
	}

	// Test inserts
	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")
	for i := 0; ; i++ {
		cnt := countWhiskies("")
		if cnt == 5 {
			break
		} else if i > 10 {
			t.Error("Expected 5 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	newCurPos := currentPosition()
	if newCurPos <= curPos {
		t.Error("Expected LSN to be greater than ", curPos, " after inserts")
	}
	curPos = newCurPos

	// Test updates
	pgConn.Exec(context.Background(), "UPDATE public.whiskies SET age = 7, name = 'Jack Daniels 2' WHERE name = 'Jack Daniels'")
	for i := 0; ; i++ {
		if countWhiskies("WHERE name = 'Jack Daniels 2'") == 1 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' to be updated to 'Jack Daniels 2' but it wasn't")
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	newCurPos = currentPosition()
	if newCurPos <= curPos {
		t.Error("Expected LSN to be greater than ", curPos, " after updates")
	}
	curPos = newCurPos

	// Check row we inserted/updated for correct data
	var id, whiskyType, country proto.ColStr
	var age proto.ColInt32
	if err := chConn.Do(context.Background(), ch.Query{
		Body: "SELECT id, age, type, country FROM trucker.v_whiskies_flat WHERE name = 'Jack Daniels 2'",
		Result: proto.Results{
			{Name: "id", Data: &id},
			{Name: "age", Data: &age},
			{Name: "type", Data: &whiskyType},
			{Name: "country", Data: &country},
		},
	}); err != nil {
		t.Error("Failed to query v_whiskies_flat", err)
	}

	if age.Row(0) != 4 || whiskyType.Row(0) != "Bourbon" || country.Row(0) != "USA" {
		t.Error("Expected 'Jack Daniels 2' to be 4 years old, Bourbon and from USA but got ", id.Row(0), age.Row(0), whiskyType.Row(0), country.Row(0))
	}

	// Test deletes
	pgConn.Exec(context.Background(), "DELETE FROM public.whiskies WHERE name = 'Jack Daniels 2'")
	for i := 0; ; i++ {
		if countWhiskies("WHERE name = 'Jack Daniels 2'") == 0 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' row not to exist but it did")
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	newCurPos = currentPosition()
	if newCurPos <= curPos {
		t.Error("Expected LSN to be greater than ", curPos, " after updates")
	}
	curPos = newCurPos
}

func TestPostgresToClickhouseLarge(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()

	insertValues := strings.Builder{}
	insertValues.WriteString("INSERT INTO whiskies (name, age, whisky_type_id) VALUES ('Blargh', 1, 1)")
	for i := range 15000 {
		insertValues.WriteString(fmt.Sprintf(",('whisky_%d',1,2)", i))
	}
	_, err := pgConn.Exec(context.Background(), insertValues.String())
	if err != nil {
		t.Error("Couldn't insert 15k rows... ", err)
	}

	exitChan := startTrucker("postgres_to_clickhouse")

	// Test backfill
	for i := 0; ; i++ {
		var cnt proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body:   "SELECT count(*) cnt FROM trucker.v_whiskies_flat",
			Result: proto.Results{{Name: "cnt", Data: &cnt}},
		}); err != nil {
			t.Error("Failed to query v_whiskies_flat", err)
		}

		if cnt.Row(0) == 15005 {
			break
		} else if i > 10 {
			t.Error("Expected 15005 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Test updates
	pgConn.Exec(context.Background(), "UPDATE public.whiskies SET age = 77")
	for i := 0; ; i++ {
		var age, cnt proto.ColUInt64
		if err := chConn.Do(context.Background(), ch.Query{
			Body: "SELECT age::UInt64 age, count(*) cnt FROM trucker.v_whiskies_flat GROUP BY age ORDER BY age",
			Result: proto.Results{
				{Name: "age", Data: &age},
				{Name: "cnt", Data: &cnt},
			},
		}); err != nil {
			t.Error("Failed to query v_whiskies_flat", err)
		}

		allRows := make([][]uint64, 0)
		for i := range age.Rows() {
			allRows = append(allRows, []uint64{age.Row(i), cnt.Row(i)})
		}

		expectedResult := [][]uint64{
			{120, 1},
			{124, 1},
			{130, 1},
			{134, 1},
			{152, 15001},
		}

		if reflect.DeepEqual(allRows, expectedResult) {
			break
		} else if i > 10 {
			t.Errorf(`Expected age distribution:
        %v
but got %v: `, expectedResult, allRows)
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	close(exitChan)
}
