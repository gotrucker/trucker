package testharness

import (
	"fmt"
	"strings"
	"time"
)

// defaultTimeout bounds the streaming/catchup/assertion phases of a single test.
// It is a var (not a const) only so the harness self-tests can shorten it; production
// CLI runs always use 30s.
var defaultTimeout = 30 * time.Second

type Result struct {
	TruckName         string
	TestName          string
	Status            string
	Phase             string
	Error             string
	Failures          []ExpectationFailure
	Duration          time.Duration
	ObservedOutputLSN uint64
	TargetOutputLSN   uint64
}

func (r Result) Failed() bool {
	return r.Status == "fail"
}

type ExpectationFailure struct {
	StatementIndex int
	SQLPreview     string
	RowIndex       int
	ColumnName     string
	Reason         string
	Value          any
	Err            error
}

func (f ExpectationFailure) String() string {
	if f.Err != nil {
		return fmt.Sprintf("statement %d (%s): %s: %v", f.StatementIndex, f.SQLPreview, f.Reason, f.Err)
	}
	if f.Reason == "zero_rows" {
		return fmt.Sprintf("statement %d (%s): returned zero rows", f.StatementIndex, f.SQLPreview)
	}
	return fmt.Sprintf(
		"statement %d (%s): row %d column %s returned non-truthy value %v",
		f.StatementIndex,
		f.SQLPreview,
		f.RowIndex,
		f.ColumnName,
		f.Value,
	)
}

func sqlPreview(sql string) string {
	preview := strings.Join(strings.Fields(sql), " ")
	if len(preview) > 120 {
		return preview[:117] + "..."
	}
	return preview
}
