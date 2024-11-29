package config

import (
	"os"
	"testing"
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

	if truck.Input.Connection != "pgconn" {
		t.Error("Expected input connection = pgconn, got", truck.Input.Connection)
	}

	if truck.Input.Table != "public.whiskies" {
		t.Error("Expected input table = public.whiskies, got", truck.Input.Table)
	}

	if truck.Output.Connection != "chconn" {
		t.Error("Expected output connection = chconn, got", truck.Output.Connection)
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
