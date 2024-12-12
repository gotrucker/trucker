package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tonyfg/trucker/test/helpers"
)

var (
	_, b, _, _ = runtime.Caller(0)
	Basepath   = filepath.Dir(b)
)

func TestDoTheThing(t *testing.T) {
	pgConn := helpers.PreparePostgresTestDb()
	defer pgConn.Close(context.Background())
	chConn := helpers.PrepareClickhouseTestDb()
	defer chConn.Close()

	err := os.Chdir("test/fixtures/fake_project")
	if err != nil {
		t.Error(err)
	}

	go doTheThing(Basepath + "/test/fixtures/fake_project")

	i := 0
	for {
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
		i += 1
	}

	// Test inserts
	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")

	i = 0
	for {
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
		i += 1
	}

	// Test updates
	pgConn.Exec(context.Background(), "UPDATE public.whiskies SET age = 7, name = 'Jack Daniels 2' WHERE name = 'Jack Daniels'")

	i = 0
	for {
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
		i += 1
	}

	var id, age int32
	var typeName, country string
	row := chConn.QueryRow(context.Background(), "SELECT id, age, type, country FROM trucker.v_whiskies_flat WHERE name = 'Jack Daniels 2'")
	row.Scan(&id, &age, &typeName, &country)

	if age != 4 || typeName != "Bourbon" || country != "USA" {
		t.Error("Expected 'Jack Daniels 2' to be 4 years old, Bourbon and from USA but got ", id, age, typeName, country)
	}

	// Test deletes
	pgConn.Exec(context.Background(), "DELETE FROM public.whiskies WHERE name = 'Jack Daniels 2'")

	i = 0
	for {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.v_whiskies_flat WHERE id = $1 AND country IS NULL", id)
		row.Scan(&cnt)

		if cnt == 1 {
			break
		} else if i > 10 {
			t.Error("Expected 'Jack Daniels' row to exist but it didn't")
			break
		}

		time.Sleep(300 * time.Millisecond)
		i += 1
	}

	row = chConn.QueryRow(context.Background(), "SELECT name, age, type, country FROM trucker.v_whiskies_flat WHERE id = $1", id)
	var name string
	typeName = ""
	country = ""
	row.Scan(&name, &age, &typeName, &country)

	if name != "Jack Daniels 2" || age != -14 || typeName != "" || country != "" {
		t.Error("Expected Jack Daniels to have been emptied out, but got values:", name, age, typeName, country)
	}

}
