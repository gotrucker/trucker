package config

import (
	"testing"

	"os"
)

func TestLoadTrucks(t *testing.T) {
	globalCfg := Config{
		Connections: map[string]Connection{
			"pgconn": Connection{},
		},
	}

	trucks := LoadTrucks(
		"../../test/fixtures/fake_project",
		globalCfg,
	)

	if len(trucks) != 1 {
		t.Error("Expected 1 truck, got", len(trucks))
	}

	truck := trucks[0]

	if readers := globalCfg.Connections["pgconn"].ReaderCount; readers != 2 {
		// For the input db we need 2 connections: 1 for streaming and 1 for querying to enrich data.
	t.Error("Expected pgconn ReaderCount to be incremented to 2, got", readers)
	}

	if writers := globalCfg.Connections["pgconn"].WriterCount; writers != 1 {
		// For the output db we need 1 connection to do the inserts
	t.Error("Expected pgconn WriterCount to be incremented to 1, got", writers)
	}

	if truck.Strict != false {
		t.Error("Expected exit_on_error = false, got", truck.Strict)
	}

	if truck.Input.Connection != "pgconn" {
		t.Error("Expected input connection = pgconn, got", truck.Input.Connection)
	}

	if truck.Input.Table != "public.whiskies" {
		t.Error("Expected input table = public.whiskies, got", truck.Input.Table)
	}

	if truck.Output.Connection != "pgconn" {
		t.Error("Expected output connection = pgconn, got", truck.Output.Connection)
	}

	if truck.Output.Table != "public.whiskies_flat" {
		t.Error("Expected output table = public.whiskies_flat, got", truck.Output.Table)
	}

	inputSql, err := os.ReadFile("../../test/fixtures/fake_project/truck1/input.sql")
	if err != nil {
		t.Error(err)
	}

	if truck.Input.Sql != string(inputSql) {
		t.Error("Expected input sql to be ", inputSql, "got", truck.Input.Sql)
	}

	outputSql, err := os.ReadFile("../../test/fixtures/fake_project/truck1/output.sql")
	if err != nil {
		t.Error(err)
	}

	if truck.Output.Sql != string(outputSql) {
		t.Error("Expected input sql to be ", outputSql, "got", truck.Output.Sql)
	}
}
