package db

import (
	"log"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/postgres"
)

type Reader interface {
	Read(operation string, columns []string, rowValues [][]any) ([]string, [][]any)
	Close()
}

func NewReader(inputSql string, cfg config.Connection) Reader {
	switch cfg.Adapter {
	case "postgres":
		return postgres.NewReader(inputSql, cfg)
	case "clickhouse":
		log.Fatalf("Clickhouse is not supported as an input source")
	default:
		log.Fatalf("Unsupported adapter: %s", cfg.Adapter)
	}

	return nil
}
