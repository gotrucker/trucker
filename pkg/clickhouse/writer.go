package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            driver.Conn
	maxQuerySize    uint64
}

func NewWriter(inputConnectionName string, writeQuery string, cfg config.Connection) *Writer {
	tmpl, err := template.New("outputSql").Parse(writeQuery)
	if err != nil {
		panic(err)
	}

	conn := NewConnection(cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database)

	return &Writer{
		currentLsnTable: fmt.Sprintf("trucker_current_lsn__%s", inputConnectionName),
		queryTemplate:   tmpl,
		conn:            conn,
	}
}

func (w *Writer) SetupPositionTracking() {
	w.SetCurrentPosition(0)
}

func (w *Writer) SetCurrentPosition(lsn uint64) {
	if err := w.conn.Exec(
		context.Background(),
		fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT $1::UInt64 AS lsn", w.currentLsnTable),
		lsn,
	); err != nil {
		panic(err)
	}
}

func (w *Writer) GetCurrentPosition() uint64 {
	row := w.conn.QueryRow(
		context.Background(),
		fmt.Sprintf("SELECT lsn FROM %s", w.currentLsnTable),
	)

	var lsn uint64
	if err := row.Scan(&lsn); err != nil {
		return 0
	}

	return lsn
}

func (w *Writer) Write(operation uint8, columns []db.Column, values [][]any) {
	if len(columns) == 0 || len(values) == 0 {
		return
	}

	valuesLiteral, flatValues := makeValuesLiteral(columns, values)

	tmplVars := map[string]string{
		"operation": db.OperationStr(operation),
		"rows":      valuesLiteral.String(),
	}
	sql := new(bytes.Buffer)
	err := w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	var valuesLen uint64
	for _, arr := range values {
		for _, v := range arr {
			valuesLen += uint64(len(fmt.Sprintf("%v", v)))
		}
	}

	sqlStr := sql.String()
	maxQuerySize := w.getMaxQuerySize()
	if valuesLen+uint64(len(sqlStr)) > maxQuerySize {
		log.Printf(
			"[Clickhouse Writer] Query size bigger than Clickhouse max_query_size (%d > %d). Splitting into 2...",
			valuesLen+uint64(len(sqlStr)),
			maxQuerySize,
		)

		half := len(values) / 2
		w.Write(operation, columns, values[:half])
		w.Write(operation, columns, values[half:])
		return
	}

	err = w.conn.Exec(context.Background(), sqlStr, flatValues...)
	if err != nil {
		log.Printf("[Clickhouse Writer] Error executing SQL:\n%s", sqlStr)
		log.Printf("[Clickhouse Writer] Values:\n%v", flatValues)
		log.Println("[Clickhouse Writer] SQL length / Values length: ", uint64(len(sqlStr)), " / ", valuesLen)
		panic(err)
	}
}

func (w *Writer) TruncateTable(table string) {
	err := w.conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s", table))
	if err != nil {
		panic(err)
	}
}

func (w *Writer) WithTransaction(f func()) {
	f()
}

func (w *Writer) Close() {
	w.conn.Close()
}

func (w *Writer) getMaxQuerySize() uint64 {
	if w.maxQuerySize == 0 {
		row := w.conn.QueryRow(
			context.Background(),
			`SELECT value
FROM system.settings
WHERE name = 'max_query_size'`,
		)

		var strVal string
		if err := row.Scan(&strVal); err != nil {
			panic(err)
		}
		n, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			panic(err)
		}
		w.maxQuerySize = uint64(n)
	}

	return w.maxQuerySize
}
