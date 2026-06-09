package testharness

import (
	"context"
	"fmt"
	"time"

	"github.com/tonyfg/trucker/pkg/config"
)

type BarrierTimeoutError struct {
	Observed uint64
	Target   uint64
}

func (e BarrierTimeoutError) Error() string {
	return fmt.Sprintf("timed out waiting for output LSN: observed=%d target=%d", e.Observed, e.Target)
}

const barrierPollInterval = 100 * time.Millisecond

func waitForOutputLSN(ctx context.Context, conn config.Connection, tableName string, latestLSN uint64, deadline time.Time) (uint64, error) {
	return pollForOutputLSN(ctx, latestLSN, deadline, barrierPollInterval, func() (uint64, error) {
		return outputLSN(ctx, conn, tableName)
	})
}

// pollForOutputLSN repeatedly calls read until the observed LSN reaches latestLSN, the
// deadline passes, or ctx is cancelled. Read errors are tolerated (the tracking table may
// not exist yet) and simply trigger a retry. It returns the most recent successfully
// observed LSN alongside any terminal error. Factored out from waitForOutputLSN so the
// retry/timeout logic can be unit-tested without a database.
func pollForOutputLSN(ctx context.Context, latestLSN uint64, deadline time.Time, interval time.Duration, read func() (uint64, error)) (uint64, error) {
	var observed uint64
	for {
		if !time.Now().Before(deadline) {
			return observed, BarrierTimeoutError{Observed: observed, Target: latestLSN}
		}
		select {
		case <-ctx.Done():
			return observed, ctx.Err()
		default:
		}

		lsn, err := read()
		if err == nil {
			observed = lsn
			if observed >= latestLSN {
				return observed, nil
			}
		}

		time.Sleep(interval)
	}
}

func outputLSN(ctx context.Context, conn config.Connection, tableName string) (uint64, error) {
	switch conn.Adapter {
	case "postgres":
		pgConn, err := openPostgres(ctx, conn, false)
		if err != nil {
			return 0, err
		}
		defer pgConn.Close(ctx)

		var lsn uint64
		err = pgConn.QueryRow(ctx, fmt.Sprintf("SELECT lsn FROM %s", quoteIdent(tableName))).Scan(&lsn)
		return lsn, err

	case "clickhouse":
		db := openClickHouseSQL(conn)
		defer db.Close()

		var lsn uint64
		err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT lsn FROM %s.%s FINAL", quoteIdent(conn.Database), quoteIdent(tableName))).Scan(&lsn)
		return lsn, err

	default:
		return 0, fmt.Errorf("unsupported adapter %q", conn.Adapter)
	}
}
