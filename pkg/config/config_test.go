package config

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	config := Load("../../test/fixtures/projects/postgres_to_clickhouse/trucker.yml")

	if config.UniqueId != "2" {
		t.Error("Expected unique id = 2, got", config.UniqueId)
	}

	if config.SlowQueryThresholdMs != 1500 {
		t.Error("Expected slow query threshold = 1500, got", config.SlowQueryThresholdMs)
	}

	if len(config.Connections) != 2 {
		t.Error("Expected 2 connections, got", len(config.Connections))
	}

	conn := config.Connections["pg_input_conn"]

	if conn.Name != "pg_input_conn" {
		t.Error("Expected connection name = pg_input_conn, got", conn.Name)
	}

	if conn.Adapter != "postgres" {
		t.Error("Expected connection adapter = postgres, got", conn.Adapter)
	}

	if conn.Host != "pg_input" {
		t.Error("Expected connection host = pg_input, got", conn.Host)
	}

	if conn.Port != 0 {
		t.Error("Expected connection port = 0, got", conn.Port)
	}

	if conn.Ssl != "prefer" {
		t.Error("Expected connection SSL = prefer, got", conn.Ssl)
	}

	if conn.Database != "trucker" {
		t.Error("Expected connection database = trucker, got", conn.Database)
	}

	if conn.User != "trucker" {
		t.Error("Expected connection user = trucker, got", conn.User)
	}

	if conn.Pass != "pgpass" {
		t.Error("Expected connection pass = pgpass, got", conn.Pass)
	}

	conn = config.Connections["chconn"]

	if conn.Name != "chconn" {
		t.Error("Expected connection name = chconn, got", conn.Name)
	}

	if conn.Adapter != "clickhouse" {
		t.Error("Expected connection adapter = clickhouse, got", conn.Adapter)
	}

	if conn.Host != "clickhouse" {
		t.Error("Expected connection host = clickhouse, got", conn.Host)
	}

	if conn.Port != 0 {
		t.Error("Expected connection port = 0, got", conn.Port)
	}

	if conn.Database != "trucker" {
		t.Error("Expected connection database = trucker, got", conn.Database)
	}

	if conn.User != "trucker" {
		t.Error("Expected connection user = trucker, got", conn.User)
	}

	if conn.Pass != "trucker" {
		t.Error("Expected connection pass = trucker got", conn.Pass)
	}
}
