package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

func TestPostgresToPostgres(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	currentPosition := func() uint64 {
		var lsn uint64
		row := conn.QueryRow(context.Background(), "SELECT lsn FROM trucker_current_lsn__pg_input_conn")
		row.Scan(&lsn)
		return lsn
	}

	exitChan := startTrucker("postgres_to_postgres")

	// Test backfill
	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 4 {
			break
		} else if i > 10 {
			t.Error("Expected 4 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	curPos := currentPosition()
	if curPos == 0 {
		t.Error("Expected LSN to be non-zero after backfill")
	}

	// Test inserts
	conn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")
	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

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
		t.Error("Expected LSN to have increased after inserts")
	}
	curPos = newCurPos

	// Test updates
	conn.Exec(context.Background(), "UPDATE public.whiskies SET age = 7, name = 'Jack Daniels 2' WHERE name = 'Jack Daniels'")
	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat WHERE name = 'Jack Daniels 2'")
		row.Scan(&cnt)

		if cnt == 1 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' to be updated to 'Jack Daniels 2' but it wasn't")
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	newCurPos = currentPosition()
	if newCurPos <= curPos {
		t.Error("Expected LSN to have increased after updates")
	}
	curPos = newCurPos

	// Check row we inserted/updated for correct data
	var id, age int32
	var typeName, country string
	row := conn.QueryRow(context.Background(), "SELECT id, age, type, country FROM whiskies_flat WHERE name = 'Jack Daniels 2'")
	row.Scan(&id, &age, &typeName, &country)

	if age != 4 || typeName != "Bourbon" || country != "USA" {
		t.Error("Expected 'Jack Daniels 2' to be 4 years old, Bourbon and from USA but got ", id, age, typeName, country)
	}

	// Test deletes
	conn.Exec(context.Background(), "DELETE FROM public.whiskies WHERE name = 'Jack Daniels 2'")
	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat WHERE id = $1 AND country IS NULL", id)
		row.Scan(&cnt)

		if cnt == 1 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' row to exist but it didn't")
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	newCurPos = currentPosition()
	if newCurPos <= curPos {
		t.Error("Expected LSN to have increased after deletes")
	}
	curPos = newCurPos

	row = conn.QueryRow(context.Background(), "SELECT name, age, type, country FROM whiskies_flat WHERE id = $1", id)
	var name string
	typeName = ""
	country = ""
	row.Scan(&name, &age, &typeName, &country)

	if name != "Jack Daniels 2" || age != -14 || typeName != "" || country != "" {
		t.Error("Expected Jack Daniels to have been emptied out, but got values:", name, age, typeName, country)
	}

	close(exitChan)
}

func TestPostgresToPostgresLarge(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	insertValues := strings.Builder{}
	insertValues.WriteString("INSERT INTO whiskies (name, age, whisky_type_id) VALUES ('Blargh', 1, 1)")
	for i := range 15000 {
		insertValues.WriteString(fmt.Sprintf(",('whisky_%d',1,2)", i))
	}
	_, err := conn.Exec(context.Background(), insertValues.String())
	if err != nil {
		t.Error("Couldn't insert 15k rows... ", err)
	}

	exitChan := startTrucker("postgres_to_postgres")

	// Test backfill
	for i := 0; ; i++ {
		var cnt uint64
		row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat")
		row.Scan(&cnt)

		if cnt == 15005 {
			break
		} else if i > 10 {
			t.Error("Expected 15005 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Test updates
	conn.Exec(context.Background(), "UPDATE public.whiskies SET age = 77")
	for i := 0; ; i++ {
		rows, err := conn.Query(context.Background(), "SELECT age::bigint, count(*) FROM whiskies_flat GROUP BY age ORDER BY age")
		if err != nil {
			t.Error("Couldn't query Postgres... ", err)
		}

		allRows := make([][]int64, 0)
		for rows.Next() {
			var age, cnt int64
			rows.Scan(&age, &cnt)
			allRows = append(allRows, []int64{age, cnt})
		}
		rows.Close()

		expectedResult := [][]int64{
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

		time.Sleep(300 * time.Millisecond)
	}

	close(exitChan)
}
