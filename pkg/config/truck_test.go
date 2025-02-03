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
		"../../test/fixtures/projects/postgres_to_clickhouse",
		globalCfg,
	)

	if len(trucks) != 1 {
		t.Error("Expected 1 trucks, got", len(trucks))
	}

	truck := trucks[0]

	if truck.Input.Connection != "pg_input_conn" {
		t.Error("Expected input connection = pg_input_conn, got", truck.Input.Connection)
	}

	if truck.Input.Table != "public.whiskies" {
		t.Error("Expected input table = public.whiskies, got", truck.Input.Table)
	}

	if truck.Output.Connection != "chconn" {
		t.Error("Expected output connection = chconn, got", truck.Output.Connection)
	}

	inputSql, err := os.ReadFile("../../test/fixtures/projects/postgres_to_clickhouse/truck/input.sql")
	if err != nil {
		t.Error(err)
	}

	if truck.Input.Sql != string(inputSql) {
		t.Error("Expected input sql to be ", inputSql, "got", truck.Input.Sql)
	}

	outputSql, err := os.ReadFile("../../test/fixtures/projects/postgres_to_clickhouse/truck/output.sql")
	if err != nil {
		t.Error(err)
	}

	if truck.Output.Sql != string(outputSql) {
		t.Error("Expected output sql to be ", outputSql, "got", truck.Output.Sql)
	}
}
