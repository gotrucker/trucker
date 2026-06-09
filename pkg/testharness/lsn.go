package testharness

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/tonyfg/trucker/pkg/config"
)

func currentInputLSN(ctx context.Context, conn config.Connection) (uint64, error) {
	pgConn, err := openPostgres(ctx, conn, false)
	if err != nil {
		return 0, err
	}
	defer pgConn.Close(ctx)

	var inRecovery bool
	if err := pgConn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return 0, err
	}

	query := "SELECT pg_current_wal_lsn()"
	if inRecovery {
		query = "SELECT pg_last_wal_receive_lsn()"
	}

	var lsnStr string
	if err := pgConn.QueryRow(ctx, query).Scan(&lsnStr); err != nil {
		return 0, err
	}
	if lsnStr == "" {
		return 0, fmt.Errorf("current input LSN is empty")
	}
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	return uint64(lsn), err
}
