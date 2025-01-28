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
		// Is compression worth it?
		// Compression: &clickhouse.Compression{
		// 	Method: clickhouse.CompressionLZ4,
		// },
		// FIXME: This is a workaround for the CH driver not having a way for us to check out a connection and hold on to it across multiple queries.
		//        We need that to be able to use temporary tables to deal with large transactions and backfills.
		//        Once the driver implements that we can allow a higher number of open connections.
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})
	if err != nil {
		panic(err)
	}

	return conn
}
