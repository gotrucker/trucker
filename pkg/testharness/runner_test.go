package testharness

import "testing"

func TestHarnessFixturePostgresToPostgres(t *testing.T) {
	results, err := Run("../../test/fixtures/projects/test_harness", "truck", "basic_insert")
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %d", len(results))
	}
	if results[0].Status != "pass" {
		t.Fatalf("expected pass, got %#v", results[0])
	}
}
