package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	// "log"
	"text/template"

    "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/tonyfg/trucker/pkg/config"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            driver.Conn
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

func (w *Writer) SetCurrentPosition(lsn int64) {
	if err := w.conn.Exec(
		context.Background(),
		fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT $1::Int64 AS lsn", w.currentLsnTable),
		lsn,
	); err != nil {
		panic(err)
	}
}

func (w *Writer) GetCurrentPosition() int64 {
	row := w.conn.QueryRow(
		context.Background(),
		fmt.Sprintf("SELECT lsn FROM %s", w.currentLsnTable),
	)

	var lsn int64
	if err := row.Scan(&lsn); err != nil {
		return 0
	}

	return lsn
}

func (w *Writer) Write(columns []string, values [][]any) {
	if len(columns) == 0 || len(values) == 0 {
		return
	}

	valuesLiteral, flatValues := makeValuesLiteral(columns, values)

	tmplVars := map[string]string{"rows": valuesLiteral.String()}
	sql := new(bytes.Buffer)
	err := w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	// log.Printf("[Clickhouse Writer] Executing SQL:\n%s", sql.String())
	// log.Printf("[Clickhouse Writer] Values:\n%v", flatValues)
	err = w.conn.Exec(context.Background(), sql.String(), flatValues...)
	if err != nil {
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
