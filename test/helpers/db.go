package helpers

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

var (
	_, b, _, _  = runtime.Caller(0)
	Basepath    = filepath.Dir(b)
	PostgresCfg = config.Connection{
		Name:     "test_pg",
		Adapter:  "postgres",
		Host:     "pg_input",
		Port:     5432,
		Ssl:      "allow",
		Database: "trucker",
		User:     "trucker",
	}
	ClickhouseCfg = config.Connection{
		Name:     "test_ch",
		Adapter:  "clickhouse",
		Host:     "clickhouse",
		Port:     9000,
		Database: "trucker",
		User:     "trucker",
		Pass:     "trucker",
	}
)

func PreparePostgresTestDb() *pgx.Conn {
	conn := Connect(PostgresCfg)

	sql := ReadTestDbSql(PostgresCfg.Adapter)
	_, err := conn.Exec(context.Background(), sql)
	if err != nil {
		panic(err)
	}

	return conn
}

func PrepareClickhouseTestDb() driver.Conn {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", ClickhouseCfg.Host, ClickhouseCfg.Port)},
		Auth: clickhouse.Auth{
			Database: ClickhouseCfg.Database,
			Username: ClickhouseCfg.User,
			Password: ClickhouseCfg.Pass,
		},
	})
	if err != nil {
		panic(err)
	}

	sql := ReadTestDbSql(ClickhouseCfg.Adapter)
	stmts := strings.Split(sql, ";\n\n")

	for _, stmt := range stmts {
		err = conn.Exec(context.Background(), stmt)
		if err != nil {
			panic(err)
		}
	}

	return conn
}

func Connect(connectionCfg config.Connection) *pgx.Conn {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		url.QueryEscape(connectionCfg.User),
		url.QueryEscape(connectionCfg.Pass),
		url.QueryEscape(connectionCfg.Host),
		connectionCfg.Port,
		url.QueryEscape(connectionCfg.Database))

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse connection string: %v\n", err)
		os.Exit(1)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}

	return conn
}

func ReadTestDbSql(adapter string) string {
	path := filepath.Join(Basepath, fmt.Sprintf("../fixtures/%s_test_db.sql", adapter))
	schema, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(schema)
}
