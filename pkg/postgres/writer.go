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

	tmplVars := map[string]string{
		"operation":   db.OperationStr(changeset.Operation),
		"input_table": changeset.Table,
	}

	columnsLiteral := makeColumnsList(changeset.Columns).String()
	valuesList, flatValues, excessRows := makeValuesListFromRowChan(changeset.Columns, changeset.Rows, [][]any{}, true)

	if len(flatValues) == 0 {
		return
	} else if len(excessRows) > 0 {
		log.Println("[Postgres Writer] Writing changeset with more than 32k parameters. Using temporary table...")
		w.prepareTempTable(conn, changeset, columnsLiteral, flatValues, excessRows)
		defer conn.Exec(context.Background(), "DROP TABLE r")
		flatValues = nil
		tmplVars["rows"] = "r"
	} else {
		sb := strings.Builder{}
		sb.WriteString("(VALUES ")
		sb.WriteString(valuesList.String())
		sb.WriteString(") AS r (")
		sb.WriteString(columnsLiteral)
		sb.WriteByte(')')
		tmplVars["rows"] = sb.String()
	}

	sql := new(bytes.Buffer)
	err = w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec(context.Background(), sql.String(), flatValues...)
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

func (w *Writer) prepareTempTable(conn *pgxpool.Conn, changeset *db.ChanChangeset, columnsLiteral string, flatValues []any, extraRows [][]any) {
	// Create a temporary table to store the rows
	sb := strings.Builder{}
	sb.WriteString("CREATE TEMPORARY TABLE r (")
	for i, col := range changeset.Columns {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("%s %s", col.Name, dbTypeToPgType(col.Type)))
	}
	sb.WriteByte(')')

	_, err := conn.Exec(context.Background(), sb.String())
	if err != nil {
		log.Printf("[Postgres Reader] Error executing SQL:\n%s", sb.String())
		panic(err)
	}

	baseSql := fmt.Sprintf("INSERT INTO r (%s) VALUES ", columnsLiteral)
	previousValuesLen := 0
	valuesList := &strings.Builder{}
	valuesList.WriteByte('(')

	for i := range len(flatValues) {
		if i > 0 {
			if i%len(changeset.Columns) == 0 {
				valuesList.WriteString("),(")
			} else {
				valuesList.WriteByte(',')
			}
		}

		valuesList.WriteString(fmt.Sprintf("$%d", i+1))
	}
	valuesList.WriteByte(')')

	for {
		if sb.Len() == 0 || len(flatValues) != previousValuesLen {
			sb = strings.Builder{}
			sb.WriteString(baseSql)
			sb.WriteString(valuesList.String())
		}

		previousValuesLen = len(flatValues)
		_, err = conn.Exec(context.Background(), sb.String(), flatValues...)
		if err != nil {
			log.Printf("[Postgres Writer] Error inserting into temporary table:\n%s", sb.String())
			log.Printf("[Postgres Writer] Values:\n%v", flatValues)
			panic(err)
		}

		// TODO [PERFORMANCE] Is there a way to avoid rebuilding valuesList on every iteration?
		valuesList, flatValues, extraRows = makeValuesListFromRowChan(changeset.Columns, changeset.Rows, extraRows, false)

		if len(flatValues) == 0 {
			break
		}
	}
}
