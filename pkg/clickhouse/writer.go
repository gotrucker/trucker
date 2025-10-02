package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"text/template"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type Writer struct {
	currentLsnTable string
	queryTemplate   *template.Template
	conn            *chpool.Pool
	maxQuerySize    int
	cfg             config.Connection
}

func NewWriter(inputConnectionName string, writeQuery string, cfg config.Connection, uniqueId string) *Writer {
	tmpl, err := template.New("outputSql").Parse(writeQuery)
	if err != nil {
		panic(err)
	}

	conn := NewConnection(cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Database)

	return &Writer{
		// FIXME: LSN tracking should be done per-truck, since writing the same
		// change on multiple trucks can be interruped midway through
		currentLsnTable: fmt.Sprintf(`"%s"."trucker_current_lsn__%s%s"`, cfg.Database, inputConnectionName, uniqueId),
		queryTemplate:   tmpl,
		conn:            conn,
		cfg:             cfg,
	}
}

func (w *Writer) SetupPositionTracking() {
	ctx := context.Background()
	w.chDo(ctx, ch.Query{
		Body: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
id Boolean DEFAULT true,
lsn UInt64
)
ENGINE = ReplacingMergeTree(lsn)
ORDER BY (id)`, w.currentLsnTable),
	})

	w.chDo(ctx, ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (lsn) VALUES (0)", w.currentLsnTable),
	})
}

func (w *Writer) SetCurrentPosition(lsn uint64) {
	w.chDo(context.Background(), ch.Query{
		Body: fmt.Sprintf("INSERT INTO %s (lsn) VALUES (%d)", w.currentLsnTable, lsn),
	})
}

func (w *Writer) GetCurrentPosition() uint64 {
	var lsn proto.ColUInt64

	if err := w.conn.Do(context.Background(), ch.Query{
		Body:   fmt.Sprintf("SELECT lsn FROM %s FINAL", w.currentLsnTable),
		Result: proto.Results{{Name: "lsn", Data: &lsn}},
	}); err != nil {
		return 0
	}

	return lsn.Row(0)
}

func (w *Writer) Write(changeset *db.Change) bool {
	ctx := context.Background()
	conn, err := w.conn.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Release()

	if !populateTempTable(ctx, conn, changeset) {
		return false
	}
	defer conn.Do(ctx, ch.Query{Body: "DROP TEMPORARY TABLE IF EXISTS r"})

	tmplVars := map[string]string{
		"operation":   db.OperationStr(changeset.Operation),
		"input_table": changeset.Table,
		"rows":        "r", // need this to help calculations for maxQuerySize
	}

	sql := new(bytes.Buffer)
	err = w.queryTemplate.Execute(sql, tmplVars)
	if err != nil {
		panic(err)
	}

	err = conn.Do(ctx, ch.Query{Body: sql.String()})
	if err != nil {
		panic(err)
	}

	return true
}

func (w *Writer) TruncateTable(table string) {
	w.chDo(context.Background(), ch.Query{
		Body:   fmt.Sprintf("TRUNCATE TABLE %s", table),
		Result: proto.Results{},
	})
}

func (w *Writer) Close() {
	w.conn.Close()
}

func populateTempTable(ctx context.Context, conn *chpool.Client, changeset *db.Change) bool {
	tableCreated := false

	for batch := range changeset.Rows {
		if !tableCreated {
			createTempTable(ctx, conn, changeset)
			tableCreated = true
		}

		values := make(map[string]proto.ColInput)

		for _, row := range batch {
			for i, col := range changeset.Columns {
				appendValue(values, col, row, i)
			}
		}

		var block proto.Input
		for name, col := range values {
			block = append(block, proto.InputColumn{Name: name, Data: col})
		}

		err := conn.Do(ctx, ch.Query{Body: "INSERT INTO r VALUES", Input: block})
		if err != nil {
			panic(err)
		}
	}

	return tableCreated
}

func createTempTable(ctx context.Context, conn *chpool.Client, changeset *db.ChanChangeset) {
	sb := strings.Builder{}
	sb.WriteString("CREATE TEMPORARY TABLE r (")
	sb.WriteString(makeColumnTypesSql(changeset.Columns).String())
	sb.WriteByte(')')

	err := conn.Do(ctx, ch.Query{Body: sb.String(), Result: proto.Results{}})
	if err != nil {
		panic(err)
	}
}

func appendValue(values map[string]proto.ColInput, col db.Column, row []any, i int) {
	if values[col.Name] == nil {
		switch col.Type {
		case db.Int8:
			values[col.Name] = &proto.ColInt8{}
		case db.Int16:
			values[col.Name] = &proto.ColInt16{}
		case db.Int32:
			values[col.Name] = &proto.ColInt32{}
		case db.Int64:
			values[col.Name] = &proto.ColInt64{}
		case db.UInt8:
			values[col.Name] = &proto.ColUInt8{}
		case db.UInt16:
			values[col.Name] = &proto.ColUInt16{}
		case db.UInt32:
			values[col.Name] = &proto.ColUInt32{}
		case db.UInt64:
			values[col.Name] = &proto.ColUInt64{}
		case db.Numeric:
			values[col.Name] = &proto.ColDecimal256{}
		case db.Float32:
			values[col.Name] = &proto.ColFloat32{}
		case db.Float64:
			values[col.Name] = &proto.ColFloat64{}
		case db.Bool:
			values[col.Name] = &proto.ColBool{}
		case db.String:
			values[col.Name] = &proto.ColStr{}
		case db.Date:
			values[col.Name] = &proto.ColDate32{}
		case db.DateTime:
			values[col.Name] = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		case db.IPAddr:
			values[col.Name] = &proto.ColIPv4{} //FIXME: What about IPv6?

		case db.MapStringToString:
			values[col.Name] = proto.NewMap(&proto.ColStr{}, &proto.ColStr{})

		case db.Int8Array:
			values[col.Name] = proto.NewArrInt8()
		case db.Int16Array:
			values[col.Name] = proto.NewArrInt16()
		case db.Int32Array:
			values[col.Name] = proto.NewArrInt32()
		case db.Int64Array:
			values[col.Name] = proto.NewArrInt64()
		case db.UInt8Array:
			values[col.Name] = proto.NewArrUInt8()
		case db.UInt16Array:
			values[col.Name] = proto.NewArrUInt16()
		case db.UInt32Array:
			values[col.Name] = proto.NewArrUInt32()
		case db.UInt64Array:
			values[col.Name] = proto.NewArrUInt64()
		case db.NumericArray:
			values[col.Name] = proto.NewArrDecimal256()
		case db.Float32Array:
			values[col.Name] = proto.NewArrFloat32()
		case db.Float64Array:
			values[col.Name] = proto.NewArrFloat64()
		case db.BoolArray:
			values[col.Name] = proto.NewArray(&proto.ColBool{})
		case db.StringArray:
			values[col.Name] = proto.NewArray(&proto.ColStr{})
		case db.DateArray:
			values[col.Name] = proto.NewArrDate32()
		case db.DateTimeArray:
			values[col.Name] = proto.NewArray(
				new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
			)
		case db.IPAddrArray:
			values[col.Name] = proto.NewArrIPv4()
		case db.MapStringToStringArray:
			values[col.Name] = proto.NewArray(
				proto.NewMap(&proto.ColStr{}, &proto.ColStr{}),
			)

		default:
			// We don't know what it is, so we treat it as a string. Is this reasonable?
			values[col.Name] = &proto.ColStr{}
		}
	}

	switch col.Type {
	case db.Int8:
		values[col.Name].(*proto.ColInt8).Append(row[i].(int8))
	case db.Int16:
		values[col.Name].(*proto.ColInt16).Append(row[i].(int16))
	case db.Int32:
		values[col.Name].(*proto.ColInt32).Append(row[i].(int32))
	case db.Int64:
		values[col.Name].(*proto.ColInt64).Append(row[i].(int64)) // TODO will this really work for int when it's 32 bit?
	case db.UInt8:
		values[col.Name].(*proto.ColUInt8).Append(row[i].(uint8))
	case db.UInt16:
		values[col.Name].(*proto.ColUInt16).Append(row[i].(uint16))
	case db.UInt32:
		values[col.Name].(*proto.ColUInt32).Append(row[i].(uint32))
	case db.UInt64:
		values[col.Name].(*proto.ColUInt64).Append(row[i].(uint64)) // TODO will this really work for uint when it's 32 bit?
	case db.Numeric:
		values[col.Name].(*proto.ColDecimal256).Append(row[i].(proto.Decimal256)) // TODO test this! I'm not sure whatever value we're getting is directly convertible to proto.Decimal256
	case db.Float32:
		values[col.Name].(*proto.ColFloat32).Append(row[i].(float32))
	case db.Float64:
		values[col.Name].(*proto.ColFloat64).Append(row[i].(float64))
	case db.Bool:
		values[col.Name].(*proto.ColBool).Append(row[i].(bool))
	case db.String:
		values[col.Name].(*proto.ColStr).Append(row[i].(string))
	case db.Date:
		values[col.Name].(*proto.ColDate32).Append(row[i].(time.Time))
	case db.DateTime:
		values[col.Name].(*proto.ColDateTime64).Append(row[i].(time.Time))
	case db.IPAddr:
		values[col.Name].(*proto.ColIPv4).Append(row[i].(proto.IPv4)) //FIXME: What about IPv6?

	case db.MapStringToString:
		values[col.Name].(*proto.ColMap[string, string]).Append(row[i].(map[string]string))

	case db.Int8Array:
		slice := make([]int8, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(int8)
		}
		values[col.Name].(*proto.ColArr[int8]).Append(slice)
	case db.Int16Array:
		slice := make([]int16, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(int16)
		}
		values[col.Name].(*proto.ColArr[int16]).Append(slice)
	case db.Int32Array:
		slice := make([]int32, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(int32)
		}
		values[col.Name].(*proto.ColArr[int32]).Append(slice)
	case db.Int64Array:
		slice := make([]int64, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(int64) // TODO will this really work for int when it's 32 bit?
		}
		values[col.Name].(*proto.ColArr[int64]).Append(slice)
	case db.UInt8Array:
		slice := make([]uint8, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(uint8)
		}
		values[col.Name].(*proto.ColArr[uint8]).Append(slice)
	case db.UInt16Array:
		slice := make([]uint16, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(uint16)
		}
		values[col.Name].(*proto.ColArr[uint16]).Append(slice)
	case db.UInt32Array:
		slice := make([]uint32, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(uint32)
		}
		values[col.Name].(*proto.ColArr[uint32]).Append(slice)
	case db.UInt64Array:
		slice := make([]uint64, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(uint64) // TODO will this really work for uint when it's 32 bit?
		}
		values[col.Name].(*proto.ColArr[uint64]).Append(slice)
	case db.NumericArray:
		slice := make([]proto.Decimal256, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(proto.Decimal256) // TODO test this! I'm not sure whatever value we're getting is directly convertible to proto.Decimal256
		}
		values[col.Name].(*proto.ColArr[proto.Decimal256]).Append(slice)
	case db.Float32Array:
		slice := make([]float32, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(float32)
		}
		values[col.Name].(*proto.ColArr[float32]).Append(slice)
	case db.Float64Array:
		slice := make([]float64, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(float64)
		}
		values[col.Name].(*proto.ColArr[float64]).Append(slice)
	case db.BoolArray:
		slice := make([]bool, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(bool)
		}
		values[col.Name].(*proto.ColArr[bool]).Append(slice)
	case db.StringArray:
		slice := make([]string, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(string)
		}
		values[col.Name].(*proto.ColArr[string]).Append(slice)
	case db.DateArray:
		slice := make([]time.Time, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(time.Time)
		}
		values[col.Name].(*proto.ColArr[time.Time]).Append(slice)
	case db.DateTimeArray:
		slice := make([]time.Time, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(time.Time)
		}
		values[col.Name].(*proto.ColArr[time.Time]).Append(slice)
	case db.IPAddrArray:
		slice := make([]proto.IPv4, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(proto.IPv4) //FIXME: What about IPv6?
		}
		values[col.Name].(*proto.ColArr[proto.IPv4]).Append(slice)
	case db.MapStringToStringArray:
		slice := make([]map[string]string, len(row[i].([]any)))
		for i, v := range row[i].([]any) {
			slice[i] = v.(map[string]string) // FIXME I think a map assertion isn't going to work
		}
		values[col.Name].(*proto.ColArr[map[string]string]).Append(slice)

	default:
		// We don't know what it is, so we treat it as a string. Is this reasonable?
		values[col.Name].(*proto.ColStr).Append(row[i].(string))
	}
}

func (w *Writer) chDo(ctx context.Context, query ch.Query) {
	if err := w.conn.Do(ctx, query); err != nil {
		log.Printf("[Clickhouse Writer] Error executing SQL:\n%s", query.Body)
		panic(err)
	}
}
