package testharness

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

func deriveTestConnection(conn config.Connection) config.Connection {
	conn.Database = conn.Database + "_test"
	return conn
}

func ensureTestDB(ctx context.Context, conn config.Connection) error {
	switch conn.Adapter {
	case "postgres":
		maintenance := conn
		maintenance.Database = "postgres"
		pgConn, err := openPostgres(ctx, maintenance, false)
		if err != nil {
			return err
		}
		defer pgConn.Close(ctx)

		var exists bool
		if err := pgConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", conn.Database).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}

		if _, err := pgConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", quoteIdent(conn.Database))); err != nil {
			return err
		}
		log.Printf("Created Postgres test database %s", conn.Database)
		return nil

	case "clickhouse":
		maintenance := conn
		maintenance.Database = "default"
		db := openClickHouseSQL(maintenance)
		defer db.Close()
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdent(conn.Database))); err != nil {
			return err
		}
		return nil

	default:
		return fmt.Errorf("unsupported adapter %q", conn.Adapter)
	}
}

func cleanupInputDB(ctx context.Context, conn config.Connection, tables []string, slotPrefix string) error {
	if conn.Adapter != "postgres" {
		return fmt.Errorf("unsupported input adapter %q", conn.Adapter)
	}
	pgConn, err := openPostgres(ctx, conn, false)
	if err != nil {
		return err
	}
	defer pgConn.Close(ctx)

	for _, table := range tables {
		if _, err := pgConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteQualifiedIdent(table))); err != nil {
			return err
		}
	}
	if _, err := pgConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", quoteIdent(slotPrefix))); err != nil {
		return err
	}
	for _, slotName := range []string{slotPrefix, slotPrefix + "_temp"} {
		if err := dropReplicationSlot(ctx, pgConn, slotName); err != nil {
			return err
		}
	}
	return nil
}

func cleanupOutputDB(ctx context.Context, conn config.Connection, lsnTable string) error {
	switch conn.Adapter {
	case "postgres":
		pgConn, err := openPostgres(ctx, conn, false)
		if err != nil {
			return err
		}
		defer pgConn.Close(ctx)

		rows, err := pgConn.Query(ctx, `SELECT n.nspname, c.relname, c.relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%'
  AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')`)
		if err != nil {
			return err
		}
		defer rows.Close()

		drops := make([]string, 0)
		for rows.Next() {
			var schema, name string
			var relkind byte
			if err := rows.Scan(&schema, &name, &relkind); err != nil {
				return err
			}
			kind := postgresDropKind(relkind)
			drops = append(drops, fmt.Sprintf("DROP %s IF EXISTS %s CASCADE", kind, quoteIdent(schema)+"."+quoteIdent(name)))
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, drop := range drops {
			if _, err := pgConn.Exec(ctx, drop); err != nil {
				return err
			}
		}
		_, err = pgConn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(lsnTable)))
		return err

	case "clickhouse":
		db := openClickHouseSQL(conn)
		defer db.Close()
		rows, err := db.QueryContext(ctx, "SELECT name FROM system.tables WHERE database = ?", conn.Database)
		if err != nil {
			return err
		}
		defer rows.Close()

		tables := make([]string, 0)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			tables = append(tables, name)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, name := range tables {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", quoteIdent(conn.Database), quoteIdent(name))); err != nil {
				return err
			}
		}
		_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", quoteIdent(conn.Database), quoteIdent(lsnTable)))
		return err

	default:
		return fmt.Errorf("unsupported adapter %q", conn.Adapter)
	}
}

func dropReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	var activePID sql.NullInt64
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := conn.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&activePID); err != nil {
		return err
	}
	if activePID.Valid {
		if _, err := conn.Exec(ctx, "SELECT pg_terminate_backend($1)", activePID.Int64); err != nil {
			return err
		}
	}
	_, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
	return err
}

func openPostgres(ctx context.Context, conn config.Connection, replication bool) (*pgx.Conn, error) {
	pgPort := conn.Port
	if pgPort == 0 {
		pgPort = 5432
	}
	params := make([]string, 0)
	if conn.Ssl != "" {
		params = append(params, "sslmode="+url.QueryEscape(conn.Ssl))
	}
	if replication {
		params = append(params, "replication=database")
	}
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		url.QueryEscape(conn.User),
		url.QueryEscape(conn.Pass),
		url.QueryEscape(conn.Host),
		pgPort,
		url.QueryEscape(conn.Database),
		strings.Join(params, "&"),
	)
	return pgx.Connect(ctx, connStr)
}

func openClickHouseSQL(conn config.Connection) *sql.DB {
	port := conn.Port
	if port == 0 {
		port = 9000
	}
	return clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", conn.Host, port)},
		Auth: clickhouse.Auth{
			Database: conn.Database,
			Username: conn.User,
			Password: conn.Pass,
		},
	})
}

func postgresDropKind(relkind byte) string {
	switch relkind {
	case 'v':
		return "VIEW"
	case 'm':
		return "MATERIALIZED VIEW"
	case 'S':
		return "SEQUENCE"
	case 'f':
		return "FOREIGN TABLE"
	default:
		return "TABLE"
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteQualifiedIdent(s string) string {
	parts := strings.Split(s, ".")
	for i, part := range parts {
		parts[i] = quoteIdent(part)
	}
	return strings.Join(parts, ".")
}
