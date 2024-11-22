package pg

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ConnectionPool pgxpool.Pool

const defaultSliceCapacity = 32
const minimumPoolSize = 2

func NewConnectionPool(user string, pass string, host string, port uint16, database string) *ConnectionPool {
	if port == 0 {
		port = 5432
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, pass, host, port, database)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse connection string: %v\n", err)
		os.Exit(1)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}

	return (*ConnectionPool)(pool)
}

func (pool *ConnectionPool) Query(sql string, args ...any) ([]map[string]any, error) {
	conn, err := (*pgxpool.Pool)(pool).Acquire(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to acquire connection: %v\n", err)
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(context.Background(), sql, args...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		return nil, err
	}
	defer rows.Close()

	// convert pgx.Rows to an array of map[string]string
	// how to get the typemap in case we turn out to need it: , conn.Conn().TypeMap()
	return scanRows(rows)
}

func (pool *ConnectionPool) Disconnect() {
	(*pgxpool.Pool)(pool).Close()
}

func (pool *ConnectionPool) ConcretePool() any {
	return pool
}

func scanRows(rows pgx.Rows) ([]map[string]any, error) {
	var cols []pgconn.FieldDescription = nil
	scannedRows := make([]map[string]any, 0, defaultSliceCapacity)

	for rows.Next() {
		if cols == nil {
			cols = rows.FieldDescriptions()
		}

		values, err := rows.Values()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Scanning row values failed: %v\n", err)
			return nil, err
		}

		scannedRows = append(scannedRows, rowToStringMap(cols, values))
	}

	return scannedRows, nil
}

func rowToStringMap(cols []pgconn.FieldDescription, values []any) map[string]any {
	rowMap := make(map[string]any, len(cols))

	for i, field := range cols {
		rowMap[field.Name] = values[i]
	}

	return rowMap
}
