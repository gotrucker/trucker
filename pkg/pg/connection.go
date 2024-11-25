package pg

import (
	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/jackc/pgx/v5"
)

const defaultSliceCapacity = 32
const minimumPoolSize = 2

func NewConnection(user string, pass string, host string, port uint16, database string, replication bool) *pgx.Conn {
	if port == 0 {
		port = 5432
	}

	replicationParam := ""
	if replication {
		replicationParam = "?replication=database"
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s%s",
		user,
		url.QueryEscape(pass),
		host,
		port,
		database,
		replicationParam)

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		log.Fatalln("Unable to parse connection string:", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalln("Unable to connect to postgres server:", err)
	}

	return conn
}
