package testharness

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBarrierTimeoutError(t *testing.T) {
	err := BarrierTimeoutError{Observed: 10, Target: 20}
	if err.Error() == "" {
		t.Fatal("expected error string")
	}
}

func TestPollForOutputLSN_AlreadyCaughtUp(t *testing.T) {
	calls := 0
	observed, err := pollForOutputLSN(
		context.Background(),
		100,
		time.Now().Add(time.Second),
		time.Millisecond,
		func() (uint64, error) {
			calls++
			return 150, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed != 150 {
		t.Fatalf("observed = %d, want 150", observed)
	}
	if calls != 1 {
		t.Fatalf("expected to return on first read, got %d calls", calls)
	}
}

func TestPollForOutputLSN_StaleThenAdvances(t *testing.T) {
	values := []uint64{10, 50, 99, 100}
	idx := 0
	observed, err := pollForOutputLSN(
		context.Background(),
		100,
		time.Now().Add(2*time.Second),
		time.Millisecond,
		func() (uint64, error) {
			v := values[idx]
			if idx < len(values)-1 {
				idx++
			}
			return v, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed != 100 {
		t.Fatalf("observed = %d, want 100", observed)
	}
}

func TestPollForOutputLSN_ToleratesReadErrors(t *testing.T) {
	calls := 0
	observed, err := pollForOutputLSN(
		context.Background(),
		5,
		time.Now().Add(2*time.Second),
		time.Millisecond,
		func() (uint64, error) {
			calls++
			if calls < 3 {
				return 0, errors.New("table does not exist yet")
			}
			return 5, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed != 5 {
		t.Fatalf("observed = %d, want 5", observed)
	}
	if calls < 3 {
		t.Fatalf("expected at least 3 reads, got %d", calls)
	}
}

func TestPollForOutputLSN_TimeoutReportsLatestObserved(t *testing.T) {
	observed, err := pollForOutputLSN(
		context.Background(),
		100,
		time.Now().Add(30*time.Millisecond),
		time.Millisecond,
		func() (uint64, error) {
			return 42, nil // never reaches the target
		},
	)
	var timeoutErr BarrierTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("expected BarrierTimeoutError, got %v", err)
	}
	if timeoutErr.Observed != 42 || timeoutErr.Target != 100 {
		t.Fatalf("timeout error = %+v, want observed=42 target=100", timeoutErr)
	}
	if observed != 42 {
		t.Fatalf("observed = %d, want 42", observed)
	}
}

func TestPollForOutputLSN_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pollForOutputLSN(
		ctx,
		100,
		time.Now().Add(time.Second),
		time.Millisecond,
		func() (uint64, error) {
			return 0, nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
