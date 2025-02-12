package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
)

func NewConnection(user string, pass string, host string, port uint16, database string) *ch.Client {
	if port == 0 {
		port = 9000
	}

	conn, err := ch.Dial(
		context.Background(),
		ch.Options{
			Address:    fmt.Sprintf("%s:%d", host, port),
			Database:   database,
			User:       user,
			Password:   pass,
			ClientName: "trucker",
		},
	)
	if err != nil {
		panic(err)
	}

	return conn
}
