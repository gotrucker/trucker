package helpers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

var (
    _, b, _, _ = runtime.Caller(0)
    Basepath   = filepath.Dir(b)
)


func Connect(connectionCfg config.Connection) *pgx.Conn {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		connectionCfg.User,
		connectionCfg.Pass,
		connectionCfg.Host,
		connectionCfg.Port,
		connectionCfg.Database)

	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		panic(err)
	}

	return conn
}


func LoadTestDb(conn *pgx.Conn) {
	ClearTestDb(conn)
	sql := ReadTestDbSql()
	result := conn.PgConn().Exec(context.Background(), sql)
	result.ReadAll()
}


func ClearTestDb(conn *pgx.Conn) {
	result := conn.PgConn().Exec(
		context.Background(),
		"DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
	result.ReadAll()
}


func ReadTestDbSql() string {
	path := filepath.Join(Basepath, "../fixtures/fake_project/test_db.sql")
	schema, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(schema)
}
