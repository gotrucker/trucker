package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func NewConnection(user string, pass string, host string, port uint16, database string) driver.Conn {
	if port == 0 {
		port = 9000
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: pass,
		},
	})
	if err != nil {
		panic(err)
	}

	return conn
}
