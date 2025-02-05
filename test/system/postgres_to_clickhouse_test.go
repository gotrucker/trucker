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

func TestPostgresToClickhouse(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()

	exitChan := startTrucker("postgres_to_clickhouse")

	// Test backfill
	for i := 0; ; i++ {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.v_whiskies_flat")
		row.Scan(&cnt)

		if cnt == 4 {
			break
		} else if i > 10 {
			t.Error("Expected 4 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Test inserts
	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")
	for i := 0; ; i++ {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.v_whiskies_flat")
		row.Scan(&cnt)

		if cnt == 5 {
			break
		} else if i > 10 {
			t.Error("Expected 5 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	// Test updates
	pgConn.Exec(context.Background(), "UPDATE public.whiskies SET age = 7, name = 'Jack Daniels 2' WHERE name = 'Jack Daniels'")
	for i := 0; ; i++ {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.v_whiskies_flat WHERE name = 'Jack Daniels 2'")
		row.Scan(&cnt)

		if cnt == 1 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' to be updated to 'Jack Daniels 2' but it wasn't")
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Check row we inserted/updated for correct data
	var id, typeName, country string
	var age int32
	row := chConn.QueryRow(context.Background(), "SELECT id, age, type, country FROM trucker.v_whiskies_flat WHERE name = 'Jack Daniels 2'")
	row.Scan(&id, &age, &typeName, &country)

	if age != 4 || typeName != "Bourbon" || country != "USA" {
		t.Error("Expected 'Jack Daniels 2' to be 4 years old, Bourbon and from USA but got ", id, age, typeName, country)
	}

	// Test deletes
	pgConn.Exec(context.Background(), "DELETE FROM public.whiskies WHERE name = 'Jack Daniels 2'")
	for i := 0; ; i++ {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.v_whiskies_flat WHERE id = $1 AND country = ''", id)
		row.Scan(&cnt)

		if cnt == 1 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' row to exist but it didn't")
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	row = chConn.QueryRow(context.Background(), "SELECT name, age, type, country FROM trucker.v_whiskies_flat WHERE id = $1", id)
	var name string
	typeName = ""
	country = ""
	row.Scan(&name, &age, &typeName, &country)

	if name != "Jack Daniels 2" || age != -14 || typeName != "" || country != "" {
		t.Error("Expected Jack Daniels to have been emptied out, but got values:", name, age, typeName, country)
	}

	close(exitChan)
}

func TestPostgresToClickhouseLarge(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()

	insertValues := strings.Builder{}
	insertValues.WriteString("INSERT INTO whiskies (name, age, whisky_type_id) VALUES ('Blargh', 1, 1)")
	for i := 0; i < 15000; i++ {
		insertValues.WriteString(fmt.Sprintf(",('whisky_%d',1,2)", i))
	}
	_, err := pgConn.Exec(context.Background(), insertValues.String())
	if err != nil {
		t.Error("Couldn't insert 15k rows... ", err)
	}

	exitChan := startTrucker("postgres_to_clickhouse")

	// Test backfill
	for i := 0; ; i++ {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.v_whiskies_flat")
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
	pgConn.Exec(context.Background(), "UPDATE public.whiskies SET age = 77")
	for i := 0; ; i++ {
		rows, err := chConn.Query(context.Background(), "SELECT age::UInt64, count(*) FROM trucker.v_whiskies_flat GROUP BY age ORDER BY age")
		if err != nil {
			t.Error("Couldn't query Clickhouse... ", err)
		}

		allRows := make([][]uint64, 0)
		for rows.Next() {
			var age, cnt uint64
			rows.Scan(&age, &cnt)
			allRows = append(allRows, []uint64{age, cnt})
		}
		rows.Close()

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
