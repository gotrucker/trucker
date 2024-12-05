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
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.whiskies_flat")
		row.Scan(&cnt)

		if cnt == 4 {
			break
		} else if i > 10 {
			t.Error("Expected 4 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
		i+=1
	}

	pgConn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Jack Daniels', 5, 1)")

	i = 0
	for {
		var cnt uint64
		row := chConn.QueryRow(context.Background(), "SELECT count(*) FROM trucker.whiskies_flat")
		row.Scan(&cnt)

		if cnt == 5 {
			break
		} else if i > 10 {
			t.Error("Expected 5 rows in whiskies_flat but found ", cnt)
			break
		}

		time.Sleep(300 * time.Millisecond)
		i+=1
	}
}
