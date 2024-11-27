package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/tonyfg/trucker/test/helpers"
)

var (
	_, b, _, _    = runtime.Caller(0)
	Basepath      = filepath.Dir(b)
)

func TestDoTheThing(t *testing.T) {
	conn := helpers.PrepareTestDb()
	defer conn.Close(context.Background())

	err := os.Chdir("test/fixtures/fake_project")
	if err != nil {
		t.Error(err)
	}

	doTheThing(Basepath + "/test/fixtures/fake_project")

	var cnt int64
	row := conn.QueryRow(context.Background(), "SELECT count(*) FROM whisky_types_flat")
	row.Scan(&cnt)
	if cnt == 0 {
		t.Error("Expected some rows in whisky_types_flat but found none")
	}
}
