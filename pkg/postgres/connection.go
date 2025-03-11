package postgres

import (
	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSliceCapacity = 32
const minimumPoolSize = 2

func NewConnection(user string, pass string, host string, port uint16, ssl string, database string, replication bool) *pgxpool.Pool {
	if port == 0 {
		port = 5432
	}

	connString := fmt.Sprintf(
		"user='%s' password='%s' host='%s' port='%d' dbname='%s'",
		url.QueryEscape(user),
		url.QueryEscape(pass),
		url.QueryEscape(host),
		port,
		url.QueryEscape(database))

	if replication {
		connString += " replication='database'"
	}

	if ssl != "" {
		connString += fmt.Sprintf(" sslmode='%s'", ssl)
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatalln("Unable to parse connection string:", err)
	}

	conn, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalln("Unable to connect to postgres server:", err)
	}

	return conn
}
