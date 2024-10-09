package pg

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	// "github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ConnectionPool pgxpool.Pool

const defaultSliceCapacity = 32
const minimumPoolSize = 2

func NewConnectionPool(user string, pass string, host string, port uint16, database string, poolSize uint16) *ConnectionPool {
	if port == 0 {
		port = 5432
	}

	maxConns := enforceMinimumPoolSize(int32(poolSize))
	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, pass, host, port, database)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse connection string: %v\n", err)
		os.Exit(1)
	}
	config.MaxConns = maxConns

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

	// convert pgx.Rows to an array of map[string]string
	// how to get the typemap in case we turn out to need it: , conn.Conn().TypeMap()
	return scanRows(rows)
}

func (pool *ConnectionPool) Disconnect() {
	(*pgxpool.Pool)(pool).Close()
}

func (pool *ConnectionPool) ConcretePool() *pgxpool.Pool {
	return (*pgxpool.Pool)(pool)
}

func enforceMinimumPoolSize(maxConns int32) int32 {
	if maxConns < minimumPoolSize {
		fmt.Fprintf(
			os.Stderr,
			"WARNING: maxConns = %d, but we need at least %d. Ignoring configured value and using 2 instead...",
			maxConns,
			minimumPoolSize,
		)
		return minimumPoolSize
	}

	return maxConns
}

func scanRows(rows pgx.Rows) ([]map[string]any, error) {
	var fields []pgconn.FieldDescription = nil
	scannedRows := make([]map[string]any, 0, defaultSliceCapacity)

	for rows.Next() {
		if fields == nil {
			fields = rows.FieldDescriptions()
		}

		values, err := rows.Values()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Scanning row values failed: %v\n", err)
			return nil, err
		}

		scannedRows = append(scannedRows, rowToStringMap(fields, values))
	}

	return scannedRows, nil
}

// missing last argument if we need to uncomment the stuff below: , typeMap *pgtype.Map
func rowToStringMap(fields []pgconn.FieldDescription, values []any) map[string]any {
	rowMap := make(map[string]any, len(fields))

	for i, field := range fields {
		// We probably don't need to do any of the commented stuff below if pgx
		// can handle passing interface{} values as params to the insert
		// query...
		//
		// pgType, ok := typeMap.TypeForOID(field.DataTypeOID)
		// if !ok {
		// 	fmt.Fprintf(os.Stderr, "WARNING: Unable to find pgtype for OID %d. Treating as text\n", field.DataTypeOID)
		// }
		//
		// TODO: complete with all type OIDS from https://github.com/thoohv5/pgx/blob/master/pgtype/pgtype.go
		// switch pgType.OID {
		// case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID,
		// 	pgtype.Float4OID, pgtype.Float8OID:
		// 	rowMap[field.Name] = fmt.Sprintf("%v", values[i])
		// case pgtype.DateOID, pgtype.TimeOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		// 	rowMap[field.Name] = fmt.Sprintf("'%v'", values[i])
		// default:
		// 	// treat anything else as raw text we need to escape
		// 	// TODO: escape text and add it to the rowMap
		// }

		rowMap[field.Name] = values[i]
	}

	return rowMap
}
