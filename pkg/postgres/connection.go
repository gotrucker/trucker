package postgres

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewConnection(user string, pass string, host string, port uint16, database string, ssl string, replication bool) *pgxpool.Pool {
	connStr := connString(user, pass, host, port, database, ssl, replication)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalln("Unable to parse connection string:", err)
	}

	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalln("Unable to connect to postgres server:", err)
	}

	return conn
}

func connString(user string, pass string, host string, port uint16, database string, ssl string, replication bool) string {
	if port == 0 {
		port = 5432
	}

	params := make([]string, 0, 0)
	if ssl != "" {
		params = append(params, fmt.Sprintf("sslmode=%s", ssl))
	}
	if replication {
		params = append(params, "replication=database")
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		url.QueryEscape(user),
		url.QueryEscape(pass),
		url.QueryEscape(host),
		port,
		url.QueryEscape(database),
		strings.Join(params, "&"),
	)

	return connStr
}
