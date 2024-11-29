package postgres

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            *pgx.Conn
}

func NewWriter(inputConnectionName string, writeQuery string, cfg config.Connection) *Writer {
	tmpl, err := template.New("outputSql").Parse(writeQuery)
	if err != nil {
		panic(err)
	}

	conn := NewConnection(cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database, false)

	return &Writer{
		currentLsnTable: fmt.Sprintf("trucker_current_lsn__%s", inputConnectionName),
		queryTemplate:   tmpl,
		conn:            conn,
	}
}

func (w *Writer) SetupPositionTracking() {
	_, err := w.conn.Exec(
		context.Background(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id bool PRIMARY KEY DEFAULT true,
  lsn bigint NOT NULL,
  CONSTRAINT ensure_single_row CHECK (id)
)`, w.currentLsnTable),
	)

	if err != nil {
		panic(err)
	}
}

func (w *Writer) SetCurrentPosition(lsn int64) {
	sql := fmt.Sprintf(`INSERT INTO %s (lsn) VALUES ($1)
ON CONFLICT (id) DO UPDATE SET lsn = $1`, w.currentLsnTable)
	_, err := w.conn.Exec(context.Background(), sql, lsn)

	if err != nil {
		panic(err)
	}
}

func (w *Writer) GetCurrentPosition() int64 {
	var lsn int64
	sql := fmt.Sprintf("SELECT lsn FROM %s", w.currentLsnTable)
	row := w.conn.QueryRow(context.Background(), sql)
	row.Scan(&lsn)
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

	_, err = w.conn.Exec(context.Background(), sql.String(), flatValues...)
	if err != nil {
		panic(err)
	}
}

func (w *Writer) TruncateTable(table string) {
	_, err := w.conn.Exec(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s", table))
	if err != nil {
		panic(err)
	}
}

func (w *Writer) WithTransaction(f func()) {
	tx, err := w.conn.Begin(context.Background())
	if err != nil {
		panic(err)
	}

	f()

	err = tx.Commit(context.Background())
	if err != nil {
		panic(err)
	}
}

func (w *Writer) Close() {
	w.conn.Close(context.Background())
}
