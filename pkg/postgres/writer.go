package postgres

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            *pgxpool.Pool
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

func (w *Writer) SetCurrentPosition(lsn uint64) {
	sql := fmt.Sprintf(`INSERT INTO %s (lsn) VALUES ($1)
ON CONFLICT (id) DO UPDATE SET lsn = $1`, w.currentLsnTable)
	_, err := w.conn.Exec(context.Background(), sql, lsn)

	if err != nil {
		panic(err)
	}
}

func (w *Writer) GetCurrentPosition() uint64 {
	var lsn uint64
	sql := fmt.Sprintf("SELECT lsn FROM %s", w.currentLsnTable)
	row := w.conn.QueryRow(context.Background(), sql)
	row.Scan(&lsn)
	return lsn
}

func (w *Writer) Write(changeset *db.ChanChangeset) {
	// We need to hold on to a specific connection to be able to create and
	// access the temporary table until we're done (in case we're not using a
	// VALUES list)
	conn, err := w.conn.Acquire(context.Background())
	if err != nil {
		panic(err)
	}
	defer conn.Release()

	// FIXME: This will blow up memory on backfills of very large tables and/or
	//        streaming very large transactions.
	//        We need to look at the CH writer to get some inspiration to fix this.
	rows := make([][]any, 0)
	for rowBatch := range changeset.Rows {
		rows = append(rows, rowBatch...)
	}

	valuesLiteral, flatValues := makeValuesList(changeset.Columns, rows)
	sb := strings.Builder{}
	sb.WriteString("(VALUES ")
	sb.WriteString(valuesLiteral.String())
	sb.WriteString(") AS r (")
	sb.WriteString(makeColumnsList(changeset.Columns).String())
	sb.WriteByte(')')

	tmplVars := map[string]string{
		"operation": db.OperationStr(changeset.Operation),
		"rows":      sb.String(),
	}
	sql := new(bytes.Buffer)
	err = w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	_, err = w.conn.Exec(context.Background(), sql.String(), flatValues...)
	if err != nil {
		log.Printf("[Postgres Writer] Error running query:\n%s\n", sql.String())
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
	w.conn.Close()
}
