package config

import (
	"testing"
)

func TestLoadConfig(t *testing.T) {
	config := Load("../../test/fixtures/fake_project/trucker.yml")

	if len(config.Connections) != 2 {
		t.Error("Expected 2 connections, got", len(config.Connections))
	}

	conn := config.Connections["pgconn"]

	if conn.Name != "pgconn" {
		t.Error("Expected connection name = pgconn, got", conn.Name)
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
