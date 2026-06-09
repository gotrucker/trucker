package testharness

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"github.com/tonyfg/trucker/pkg/config"
)

type ScriptError struct {
	StatementIndex int
	SQL            string
	Err            error
}

func (e ScriptError) Error() string {
	return fmt.Sprintf("statement %d (%s): %v", e.StatementIndex, sqlPreview(e.SQL), e.Err)
}

func splitStatements(sql string) []string {
	stmts := make([]string, 0)
	start := 0
	inSingle := false
	inDouble := false
	inLineComment := false
	dollarTag := ""

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inLineComment {
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}

		if dollarTag != "" {
			if strings.HasPrefix(sql[i:], dollarTag) {
				i += len(dollarTag) - 1
				dollarTag = ""
			}
			continue
		}

		if inSingle {
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}

		if inDouble {
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}

		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inLineComment = true
			i++
			continue
		}
		if ch == '\'' {
			inSingle = true
			continue
		}
		if ch == '"' {
			inDouble = true
			continue
		}
		if ch == '$' {
			if tag, ok := readDollarTag(sql[i:]); ok {
				dollarTag = tag
				i += len(tag) - 1
			}
			continue
		}
		if ch == ';' {
			appendStatement(&stmts, sql[start:i])
			start = i + 1
		}
	}
	appendStatement(&stmts, sql[start:])
	return stmts
}

func appendStatement(stmts *[]string, stmt string) {
	stmt = strings.TrimSpace(stmt)
	if stmt != "" {
		*stmts = append(*stmts, stmt)
	}
}

func readDollarTag(s string) (string, bool) {
	if s == "" || s[0] != '$' {
		return "", false
	}
	for i := 1; i < len(s); i++ {
		if s[i] == '$' {
			return s[:i+1], true
		}
		if !(s[i] == '_' || unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i]))) {
			return "", false
		}
	}
	return "", false
}

func execScript(ctx context.Context, conn config.Connection, sql string) error {
	stmts := splitStatements(sql)
	if len(stmts) == 0 {
		return nil
	}

	switch conn.Adapter {
	case "postgres":
		pgConn, err := openPostgres(ctx, conn, false)
		if err != nil {
			return err
		}
		defer pgConn.Close(ctx)
		for i, stmt := range stmts {
			if _, err := pgConn.Exec(ctx, stmt); err != nil {
				return ScriptError{StatementIndex: i, SQL: stmt, Err: err}
			}
		}
		return nil

	case "clickhouse":
		db := openClickHouseSQL(conn)
		defer db.Close()
		for i, stmt := range stmts {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return ScriptError{StatementIndex: i, SQL: stmt, Err: err}
			}
		}
		return nil

	default:
		return fmt.Errorf("unsupported adapter %q", conn.Adapter)
	}
}
