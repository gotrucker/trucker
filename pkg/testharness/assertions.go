package testharness

import (
	"context"
	"fmt"
	"reflect"

	"github.com/tonyfg/trucker/pkg/config"
)

func runExpectations(ctx context.Context, conn config.Connection, sql string) []ExpectationFailure {
	stmts := splitStatements(sql)
	failures := make([]ExpectationFailure, 0)
	for i, stmt := range stmts {
		switch conn.Adapter {
		case "postgres":
			failures = append(failures, runPostgresExpectation(ctx, conn, i, stmt)...)
		case "clickhouse":
			failures = append(failures, runSQLExpectation(ctx, conn, i, stmt)...)
		default:
			failures = append(failures, ExpectationFailure{StatementIndex: i, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: fmt.Errorf("unsupported adapter %q", conn.Adapter)})
		}
	}
	return failures
}

func runPostgresExpectation(ctx context.Context, conn config.Connection, index int, stmt string) []ExpectationFailure {
	pgConn, err := openPostgres(ctx, conn, false)
	if err != nil {
		return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
	}
	defer pgConn.Close(ctx)

	rows, err := pgConn.Query(ctx, stmt)
	if err != nil {
		return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, field := range fields {
		cols[i] = field.Name
	}

	failures := make([]ExpectationFailure, 0)
	rowIndex := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
		}
		failures = append(failures, checkExpectationRow(index, stmt, rowIndex, cols, values)...)
		rowIndex++
	}
	if err := rows.Err(); err != nil {
		return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
	}
	if rowIndex == 0 {
		failures = append(failures, ExpectationFailure{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "zero_rows"})
	}
	return failures
}

func runSQLExpectation(ctx context.Context, conn config.Connection, index int, stmt string) []ExpectationFailure {
	db := openClickHouseSQL(conn)
	defer db.Close()

	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
	}

	failures := make([]ExpectationFailure, 0)
	rowIndex := 0
	for rows.Next() {
		values := make([]any, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
		}
		failures = append(failures, checkExpectationRow(index, stmt, rowIndex, cols, values)...)
		rowIndex++
	}
	if err := rows.Err(); err != nil {
		return []ExpectationFailure{{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "query_error", Err: err}}
	}
	if rowIndex == 0 {
		failures = append(failures, ExpectationFailure{StatementIndex: index, SQLPreview: sqlPreview(stmt), Reason: "zero_rows"})
	}
	return failures
}

func checkExpectationRow(statementIndex int, stmt string, rowIndex int, cols []string, values []any) []ExpectationFailure {
	failures := make([]ExpectationFailure, 0)
	for i, value := range values {
		columnName := fmt.Sprintf("column_%d", i)
		if i < len(cols) && cols[i] != "" {
			columnName = cols[i]
		}
		if !isTruthy(value) {
			failures = append(failures, ExpectationFailure{
				StatementIndex: statementIndex,
				SQLPreview:     sqlPreview(stmt),
				RowIndex:       rowIndex,
				ColumnName:     columnName,
				Reason:         "non_truthy_cell",
				Value:          value,
			})
		}
	}
	return failures
}

func isTruthy(value any) bool {
	if value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case int:
		return v == 1
	case int8:
		return v == 1
	case int16:
		return v == 1
	case int32:
		return v == 1
	case int64:
		return v == 1
	case uint:
		return v == 1
	case uint8:
		return v == 1
	case uint16:
		return v == 1
	case uint32:
		return v == 1
	case uint64:
		return v == 1
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Pointer && !rv.IsNil() {
		return isTruthy(rv.Elem().Interface())
	}
	return false
}
