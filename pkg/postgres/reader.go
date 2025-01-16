package postgres

import (
	"bytes"
	"context"
	"log"
	"text/template"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type Reader struct {
	queryTemplate *template.Template
	conn          *pgxpool.Pool
}

func NewReader(readQuery string, cfg config.Connection) *Reader {
	tmpl, err := template.New("inputSql").Parse(readQuery)
	if err != nil {
		panic(err)
	}

	conn := NewConnection(cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database, false)

	return &Reader{queryTemplate: tmpl, conn: conn}
}

// - transform the input args into a postgres values literal
// - feed that to the template as a .rows variable
// - run the query with values args and return the result
func (r *Reader) Read(operation uint8, columns []db.Column, rowValues [][]any) ([]db.Column, [][]any) {
	if len(columns) == 0 || len(rowValues) == 0 {
		return nil, nil
	}

	valuesLiteral, values := makeValuesLiteral(columns, rowValues)

	tmplVars := map[string]string{
		"operation": db.OperationStr(operation),
		"rows":      valuesLiteral.String(),
	}
	sql := new(bytes.Buffer)
	err := r.queryTemplate.Execute(sql, tmplVars)

	rows, err := r.conn.Query(context.Background(), sql.String(), values...)
	if err != nil {
		log.Printf("[Postgres Reader] Error running query:\n%s\n", sql.String())
		log.Printf("[Postgres Reader] Query values:\n%v\n", values)
		panic(err)
	}
	defer rows.Close()

	cols := make([]db.Column, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		cols[i] = db.Column{
			Name: field.Name,
			Type: oidToDbType(field.DataTypeOID),
		}
	}

	vals := make([][]any, 0, 1)
	for i := 0; rows.Next(); i++ {
		rowVals, err := rows.Values()
		if err != nil {
			panic(err)
		}
		vals = append(vals, rowVals)
	}

	return cols, vals
}

func (r *Reader) Close() {
	r.conn.Close()
}
