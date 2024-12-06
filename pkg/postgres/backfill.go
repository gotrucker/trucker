package postgres

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgtype"
)

const backfillBatchSize = 5000

type BackfillBatch struct {
	Columns []string
	Types   []string
	Rows    [][]any
}

func (client *ReplicationClient) StreamBackfillData(table string, snapshotName string) chan *BackfillBatch {
	changesChan := make(chan *BackfillBatch)

	go func() {
		defer close(changesChan)

		_, err := client.conn.Exec(context.Background(), "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			panic(err)
		}
		defer func() {
			_, err := client.conn.Exec(context.Background(), "ROLLBACK")
			if err != nil {
				panic(err)
			}
		}()

		_, err = client.conn.Exec(context.Background(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName))
		if err != nil {
			panic(err)
		}

		// TODO: Reading rows from the table to then use them again in the input
		//       query is not ideal. Maybe in the future we can find a way to
		//       directly use the table itself on the input query (just for the
		//       backfill of course)
		rows, err := client.conn.Query(context.Background(), "SELECT * FROM " + table)
		if err != nil {
			log.Fatalln("Query failed: ", err)
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		columns := make([]string, len(fields), len(fields) * 2)
		types := make([]string, len(fields), len(fields) * 2)
		for i, field := range fields {
			columns[i] = field.Name
			types[i] = sqlTypeFromOID(field.DataTypeOID)
		}
		columns = append(columns, addPrefix(columns, "old__")...)
		types = append(types, types...)

		cappedBatchSize := maxPreparedStatementArgs / (len(columns) + 1)
		if cappedBatchSize > backfillBatchSize {
			cappedBatchSize = backfillBatchSize
		}

		rowValues := make([][]any, 0, 1)

		for i := 0; rows.Next(); i++ {
			values, err := rows.Values()
			if err != nil {
				panic(err)
			}

			rowValues = append(rowValues, append(values, make([]any, len(fields))...))

			if i >= cappedBatchSize {
				changesChan <- &BackfillBatch{Columns: columns, Types: types, Rows: rowValues}
				rowValues = make([][]any, 0, len(rowValues))
				i = 0
			}
		}

		log.Printf("Read a batch of %d raw rows for backfill...\n", len(rowValues))
		changesChan <- &BackfillBatch{Columns: columns, Types: types, Rows: rowValues}
	}()

	return changesChan
}

func sqlTypeFromOID(oid uint32) string {
	switch oid {
	case pgtype.Int4OID:
		return "int"
	case pgtype.Int8OID:
		return "int8"
	case pgtype.Int2OID:
		return "int2"
	case pgtype.Float4OID:
		return "real"
	case pgtype.Float8OID:
		return "float"
	case pgtype.TextOID, pgtype.VarcharOID:
		return "text"
	case pgtype.BoolOID:
		return "bool"
	case pgtype.NumericOID:
		return "numeric"
	case pgtype.DateOID:
		return "date"
	case pgtype.TimestampOID:
		return "timestamp"
	case pgtype.TimestamptzOID:
		return "timestamptz"
	case pgtype.TimeOID:
		return "time"
	case pgtype.TimetzOID:
		return "timetz"
	case pgtype.UUIDOID:
		return "uuid"
	case pgtype.JSONOID:
		return "json"
	case pgtype.JSONBOID:
		return "jsonb"
	case pgtype.QCharOID:
		return `"char"`
	case pgtype.Int4ArrayOID:
		return "int[]"
	case pgtype.Int8ArrayOID:
		return "int8[]"
	case pgtype.Int2ArrayOID:
		return "int2[]"
	case pgtype.Float8ArrayOID:
		return "float[]"
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID:
		return "text[]"
	case pgtype.Float4ArrayOID:
		return "real[]"
	case pgtype.NumericArrayOID:
		return "numeric[]"
	case pgtype.BoolArrayOID:
		return "bool[]"
	case pgtype.QCharArrayOID:
		return `"char"[]`
	case pgtype.BPCharArrayOID:
		return "bpchar[]"
	case pgtype.UUIDArrayOID:
		return "uuid[]"
	case pgtype.TimestampArrayOID:
		return "timestamp[]"
	case pgtype.DateArrayOID:
		return "date[]"
	case pgtype.TimeArrayOID:
		return "time[]"
	case pgtype.TimetzArrayOID:
		return "timetz[]"
	case pgtype.TimestamptzArrayOID:
		return "timestamptz[]"
	case pgtype.JSONBArrayOID:
		return "jsonb[]"
	case pgtype.ByteaOID:
		return "bytea"
	case pgtype.XMLOID:
		return "xml"
	case pgtype.XMLArrayOID:
		return "xml[]"
	case pgtype.JSONArrayOID:
		return "json[]"
	case pgtype.PointOID:
		return "point"
	case pgtype.LsegOID:
		return "lseg"
	case pgtype.PathOID:
		return "path"
	case pgtype.BoxOID:
		return "box"
	case pgtype.PolygonOID:
		return "polygon"
	case pgtype.LineOID:
		return "line"
	case pgtype.LineArrayOID:
		return "line[]"
	case pgtype.CIDROID:
		return "cidr"
	case pgtype.CIDRArrayOID:
		return "cidr[]"
	case pgtype.CircleOID:
		return "circle"
	case pgtype.CircleArrayOID:
		return "circle[]"
	case pgtype.Macaddr8OID:
		return "macaddr8"
	case pgtype.MacaddrOID:
		return "macaddr"
	case pgtype.InetOID:
		return "inet"
	case pgtype.NameArrayOID:
		return "name[]"
	case pgtype.TIDArrayOID:
		return "tid[]"
	case pgtype.ByteaArrayOID:
		return "bytea[]"
	case pgtype.XIDArrayOID:
		return "xid[]"
	case pgtype.CIDArrayOID:
		return "cid[]"
	case pgtype.PointArrayOID:
		return "point[]"
	case pgtype.LsegArrayOID:
		return "lseg[]"
	case pgtype.PathArrayOID:
		return "path[]"
	case pgtype.BoxArrayOID:
		return "box[]"
	case pgtype.PolygonArrayOID:
		return "polygon[]"
	case pgtype.OIDArrayOID:
		return "oid[]"
	case pgtype.IntervalOID:
		return "interval"
	case pgtype.IntervalArrayOID:
		return "interval[]"
	case pgtype.BitOID:
		return "bit"
	case pgtype.BitArrayOID:
		return "bit[]"
	case pgtype.VarbitOID:
		return "varbit"
	case pgtype.VarbitArrayOID:
		return "varbit[]"
	case pgtype.RecordOID:
		return "record"
	case pgtype.RecordArrayOID:
		return "record[]"
	case pgtype.JSONPathOID:
		return "jsonpath"
	case pgtype.JSONPathArrayOID:
		return "jsonpath[]"
	default:
		return "text" // FIXME: This isn't really ok is it?
	}
}
