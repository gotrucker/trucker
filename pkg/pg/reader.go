package pg

import (
	"bytes"
	"context"
	"text/template"

	"github.com/jackc/pgx/v5"
)

type Reader struct {
	queryTemplate *template.Template
	conn          *pgx.Conn
}

func NewReader(readQuery string, pool *pgx.Conn) *Reader {
	tmpl, err := template.New("inputSql").Parse(readQuery)
	if err != nil {
		panic(err)
	}

	return &Reader{queryTemplate: tmpl, conn: pool}
}

// - transform the input args into a postgres values literal
// - feed that to the template as a .rows variable
// - run the query with values args and return the result
func (r *Reader) Read(operation string, columns []string, rowValues [][]any) ([]string, [][]any) {
	if len(columns) == 0 || len(rowValues) == 0 {
		return nil, nil
	}

	valuesLiteral, values := makeValuesLiteral(columns, rowValues)
	tmplVars := map[string]string{
		"operation": operation,
		"rows":      valuesLiteral.String(),
	}
	sql := new(bytes.Buffer)
	err := r.queryTemplate.Execute(sql, tmplVars)

	rows, err := r.conn.Query(
		context.Background(),
		sql.String(),
		values...,
	)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	cols := make([]string, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		cols[i] = field.Name
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
	r.conn.Close(context.Background())
}
