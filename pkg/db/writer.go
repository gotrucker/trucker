package db

import (
	"log"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/clickhouse"
	"github.com/tonyfg/trucker/pkg/postgres"
)

type Writer interface {
	SetupPositionTracking()
	SetCurrentPosition(lsn uint64)
	GetCurrentPosition() uint64
	Write(columns []string, values [][]any)
	TruncateTable(table string)
	WithTransaction(f func())
	Close()
}

func NewWriter(inputConnectionName string, outputSql string, cfg config.Connection) Writer {
	switch cfg.Adapter {
	case "postgres":
		return postgres.NewWriter(inputConnectionName, outputSql, cfg)
	case "clickhouse":
		return clickhouse.NewWriter(inputConnectionName, outputSql, cfg)
	default:
		log.Fatalf("Unsupported adapter: %s", cfg.Adapter)
	}

	return nil
}
