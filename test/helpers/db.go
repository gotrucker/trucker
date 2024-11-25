package helpers

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

var (
	_, b, _, _    = runtime.Caller(0)
	Basepath      = filepath.Dir(b)
	ConnectionCfg = config.Connection{
		Name:     "test",
		Adapter:  "pg",
		Host:     "pg_input",
		Port:     5432,
		Database: "trucker",
		User:     "trucker",
	}
)

func PrepareTestDb() *pgx.Conn {
	conn := Connect(ConnectionCfg)
	LoadTestDb(conn)
	return conn
}

func LoadTestDb(conn *pgx.Conn) {
	sql := ReadTestDbSql()
	conn.Exec(context.Background(), sql)
}

func Connect(connectionCfg config.Connection) *pgx.Conn {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		connectionCfg.User,
		url.QueryEscape(connectionCfg.Pass),
		connectionCfg.Host,
		connectionCfg.Port,
		connectionCfg.Database)

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

func ReadTestDbSql() string {
	path := filepath.Join(Basepath, "../fixtures/fake_project/test_db.sql")
	schema, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(schema)
}
