package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
)

func NewConnection(user string, pass string, host string, port uint16, database string) *chpool.Pool {
	if port == 0 {
		port = 9000
	}

	conn, err := chpool.Dial(
		context.Background(),
		chpool.Options{
			ClientOptions: ch.Options{
				Address:    fmt.Sprintf("%s:%d", host, port),
				Database:   database,
				User:       user,
				Password:   pass,
				ClientName: "trucker",
			},
		},
	)
	if err != nil {
		panic(err)
	}

	return conn
}
