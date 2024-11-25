package pg

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            *pgxpool.Pool
}

func NewWriter(inputConnectionName string, writeQuery string, pool *pgxpool.Pool) *Writer {
	tmpl, err := template.New("outputSql").Parse(writeQuery)
	if err != nil {
		panic(err)
	}

	return &Writer{
		currentLsnTable: fmt.Sprintf("trucker_current_lsn__%s", inputConnectionName),
		queryTemplate:   tmpl,
		conn:            pool,
	}
}

func (w *Writer) SetupLsnTracking() {
	_, err := w.conn.Exec(
		context.Background(),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id bool PRIMARY KEY DEFAULT true,
  lsn text NOT NULL,
  CONSTRAINT ensure_single_row CHECK (id)
)`, w.currentLsnTable),
	)

	if err != nil {
		panic(err)
	}
}

func (w *Writer) GetCurrentLsn() string {
	var lsn string
	sql := fmt.Sprintf("SELECT lsn FROM %s", w.currentLsnTable)
	row := w.conn.QueryRow(context.Background(), sql)
	row.Scan(&lsn)
	return lsn
}

func (w *Writer) SetCurrentLsn(lsn string) {
	sql := fmt.Sprintf(`INSERT INTO %s (lsn) VALUES ('%s')
ON CONFLICT (id) DO UPDATE SET lsn = '%s'`, w.currentLsnTable, lsn, lsn)
	_, err := w.conn.Exec(context.Background(), sql)

	if err != nil {
		panic(err)
	}
}

func (w *Writer) Write(columns []string, values [][]any) {
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


// o Reader e o Writer estão feitos e parecem bastante clean (fora a cena do mapeamento dos tipos que n está clean, mas tem lá um TODO)
// agora é preciso adaptar o replication.go e backfill.go para usar isto, e eventualmente tentar dar deprecate no connection_poool.go
