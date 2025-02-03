package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            driver.Conn
	maxQuerySize    int
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

func (w *Writer) Write(changeset *db.ChanChangeset) {
	tmplVars := map[string]string{
		"operation": db.OperationStr(changeset.Operation),
		"rows":      "VALUES('', ) r", // need this to help calculations for maxQuerySize
	}
	sql := new(bytes.Buffer)
	err := w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	typesLiteral := makeColumnTypesSql(changeset.Columns)
	baseQuerySize := sql.Len() + typesLiteral.Len()
	maxValuesListSize := w.getMaxQuerySize() - baseQuerySize

	valuesList, flatValues, excessRows := makeValuesList(changeset.Rows, maxValuesListSize, [][]any{})

	if len(flatValues) == 0 {
		log.Println("[Clickhouse Writer] Received empty changeset. Ignoring...")
		return
	} else if excessRows != nil && len(excessRows) > 0 {
		// Crap... we'll need to insert everything in batches to a temporary
		// table, and the run out output.sql from that table...
		log.Println("[Clickhouse Writer] Writing changeset with more than 256k bytes. Using temporary table...")
		w.prepareTempTable(changeset, valuesList, flatValues, excessRows, typesLiteral, maxValuesListSize)
		defer w.conn.Exec(context.Background(), "DROP TABLE r")
		flatValues = nil
		tmplVars["rows"] = "r"
	} else {
		// It fits in a single query, so let's use a VALUES list to insert
		sb := strings.Builder{}
		sb.WriteString("VALUES('")
		sb.WriteString(typesLiteral.String())
		sb.WriteString("', ")
		sb.WriteString(valuesList.String())
		sb.WriteString(") r")
		tmplVars["rows"] = sb.String()
	}

	sql = new(bytes.Buffer)
	err = w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}
	sqlStr := sql.String()

	err = w.conn.Exec(context.Background(), sqlStr, flatValues...)
	if err != nil {
		log.Printf("[Clickhouse Writer] Error executing SQL:\n%s", sqlStr)
		for i, val := range flatValues {
			log.Printf("[Clickhouse Writer] Val: %d = %v\n", i+1, val)
		}
		log.Println("[Clickhouse Writer] SQL length / Values length: ", uint64(len(sqlStr)), " / ", len(flatValues))
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

func (w *Writer) getMaxQuerySize() int {
	if w.maxQuerySize == 0 {
		row := w.conn.QueryRow(
			context.Background(),
			`SELECT value FROM system.settings WHERE name = 'max_query_size'`,
		)

		var strVal string
		if err := row.Scan(&strVal); err != nil {
			panic(err)
		}
		n, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			panic(err)
		}
		w.maxQuerySize = int(n)
	}

	return w.maxQuerySize
}

func (w *Writer) prepareTempTable(changeset *db.ChanChangeset, valuesList *strings.Builder, flatValues []any, extraRows [][]any, typesLiteral *strings.Builder, maxValuesListSize int) {
	// Create temporary table to store the rows
	sb := strings.Builder{}
	sb.WriteString("CREATE TEMPORARY TABLE r (")
	sb.WriteString(typesLiteral.String())
	sb.WriteByte(')')

	err := w.conn.Exec(context.Background(), sb.String())
	if err != nil {
		log.Printf("[Clickhouse Writer] Error executing SQL:\n%s", sb.String())
		panic(err)
	}

	sb = strings.Builder{}
	previousValuesLen := 0

	for {
		if sb.Len() == 0 || len(flatValues) != previousValuesLen {
			sb = strings.Builder{}
			sb.WriteString("INSERT INTO r (")
			for i, col := range changeset.Columns {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(col.Name)
			}
			sb.WriteString(") VALUES ")
			sb.WriteString(valuesList.String())
		}

		previousValuesLen = len(flatValues)
		err = w.conn.Exec(context.Background(), sb.String(), flatValues...)
		if err != nil {
			log.Printf("[Clickhouse Writer] Error inserting into temporary table:\n%s", sb.String())
			log.Printf("[Clickhouse Writer] Values:\n%v", flatValues)
			panic(err)
		}

		// TODO [PERFORMANCE] Is there a way to avoid rebuilding valuesList on every iteration?
		valuesList, flatValues, extraRows = makeValuesList(changeset.Rows, maxValuesListSize, extraRows)

		if len(flatValues) == 0 {
			break
		}
	}
}
