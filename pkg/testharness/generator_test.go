package testharness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "trucker.yml"), `connections:
- name: input
  adapter: postgres
  host: localhost
  database: app
  user: trucker
`)
	mustWrite(t, filepath.Join(dir, "truck", "truck.yml"), `input:
  connection: input
  table: public.events
output:
  connection: input
`)
	mustWrite(t, filepath.Join(dir, "truck", "output.sql"), `SELECT * FROM {{ .rows }}`)

	if err := Generate(dir, "truck", "basic"); err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	for _, name := range []string{"input_db_seed.sql", "output_db_seed.sql", "stream_statements.sql", "expectations.sql"} {
		path := filepath.Join(dir, "truck", "tests", "basic", name)
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
		if strings.Contains(string(contents), "{{.InputTable}}") {
			t.Fatalf("template placeholder was not replaced in %s", name)
		}
	}

	if err := Generate(dir, "truck", "basic"); err == nil {
		t.Fatal("expected existing test directory to fail")
	}
	if err := Generate(dir, "truck", "../bad"); err == nil {
		t.Fatal("expected invalid test name to fail")
	}
}

func mustWrite(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
}
