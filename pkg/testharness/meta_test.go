package testharness

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// metaExpectation mirrors the expected.json schema. Only Status is required; the other
// fields are matched only when present in the fixture's expected.json.
type metaExpectation struct {
	Status    string `json:"status"`
	Phase     string `json:"phase"`
	Statement *int   `json:"statement"`
	Reason    string `json:"reason"`
}

type metaCase struct {
	truck string
	test  string
	exp   metaExpectation
}

// TestHarnessMetaFixtures is the golden-fixture driver. It discovers every
// <truck>/tests/<test>/expected.json under test/fixtures/harness_meta, runs that single
// test through the real harness, and asserts the Result matches expected.json. The
// negative fixtures are the important ones: they prove the harness fails the *right* thing
// for the *right* reason, not just that it fails.
func TestHarnessMetaFixtures(t *testing.T) {
	original := defaultTimeout
	t.Cleanup(func() { defaultTimeout = original })

	project := copyTree(t, "../../test/fixtures/harness_meta")
	cases := discoverMetaCases(t, project)
	if len(cases) == 0 {
		t.Fatal("no harness_meta fixtures discovered")
	}

	for _, c := range cases {
		t.Run(c.truck+"/"+c.test, func(t *testing.T) {
			// The non-convergence fixture can never reach the output LSN, so keep its
			// timeout short; everything else gets a comfortable budget.
			if c.test == "failing_timeout" {
				defaultTimeout = 5 * time.Second
			} else {
				defaultTimeout = 30 * time.Second
			}

			runs := 1
			if c.test == "idempotent_rerun" {
				runs = 2
			}
			for i := 0; i < runs; i++ {
				results, err := Run(project, c.truck, c.test)
				if err != nil {
					t.Fatalf("run %d: Run returned error: %v", i, err)
				}
				if len(results) != 1 {
					t.Fatalf("run %d: expected exactly one result, got %d", i, len(results))
				}
				assertMetaExpectation(t, i, c.exp, results[0])
			}
		})
	}
}

func assertMetaExpectation(t *testing.T, run int, exp metaExpectation, r Result) {
	t.Helper()
	if r.Status != exp.Status {
		t.Fatalf("run %d: status=%q want %q (phase=%q error=%q failures=%+v)",
			run, r.Status, exp.Status, r.Phase, r.Error, r.Failures)
	}
	if exp.Phase != "" && r.Phase != exp.Phase {
		t.Fatalf("run %d: phase=%q want %q (error=%q)", run, r.Phase, exp.Phase, r.Error)
	}
	if exp.Statement != nil {
		if len(r.Failures) == 0 {
			t.Fatalf("run %d: expected a failure on statement %d but got none", run, *exp.Statement)
		}
		if r.Failures[0].StatementIndex != *exp.Statement {
			t.Fatalf("run %d: failing statement=%d want %d", run, r.Failures[0].StatementIndex, *exp.Statement)
		}
	}
	if exp.Reason != "" {
		if len(r.Failures) == 0 {
			t.Fatalf("run %d: expected failure reason %q but got none", run, exp.Reason)
		}
		if r.Failures[0].Reason != exp.Reason {
			t.Fatalf("run %d: failure reason=%q want %q", run, r.Failures[0].Reason, exp.Reason)
		}
	}
}

func discoverMetaCases(t *testing.T, project string) []metaCase {
	t.Helper()
	var cases []metaCase
	err := filepath.WalkDir(project, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "expected.json" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var exp metaExpectation
		if err := json.Unmarshal(raw, &exp); err != nil {
			return err
		}
		testDir := filepath.Dir(path)
		// .../<truck>/tests/<test>/expected.json
		truckDir := filepath.Dir(filepath.Dir(testDir))
		cases = append(cases, metaCase{
			truck: filepath.Base(truckDir),
			test:  filepath.Base(testDir),
			exp:   exp,
		})
		return nil
	})
	if err != nil {
		t.Fatalf("discovering fixtures: %v", err)
	}
	return cases
}

// copyTree copies the fixture project into a fresh temp dir so the harness never touches
// the source tree, then returns the copy's path.
func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatalf("copying fixtures: %v", err)
	}
	return dst
}
