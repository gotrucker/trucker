package postgres

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type ReplicationClient struct {
	publicationName  string
	tables           []string
	connCfg          config.Connection
	conn             *pgx.Conn
	streamConn       *pgx.Conn
	processingLSN    pglogrepl.LSN
	lastProcessedLSN pglogrepl.LSN
	running          bool
	done             chan bool
	columnsCache     map[string][]db.Column
}

func NewReplicationClient(tables []string, connCfg config.Connection, uniqueId string) *ReplicationClient {
	return &ReplicationClient{
		publicationName: fmt.Sprintf("trucker_%s%s", connCfg.Database, uniqueId),
		tables:          tables,
		connCfg:         connCfg,
		running:         false,
		done:            make(chan bool, 1),
		columnsCache:    make(map[string][]db.Column),
	}
}

func (rc *ReplicationClient) Setup() ([]string, uint64, string) {
	rc.conn = rc.connect(false) // TODO: check that this connection gets closed once we no longer need it
	rc.streamConn = rc.connect(true)
	// we need to keep the connection open so that the other connection can use
	// the repliaction slot snapshot for backfills
	// defer client.streamConn.Close(context.Background())

	for _, table := range rc.tables {
		rc.columnsCache[table] = make([]db.Column, 0, 1)

		schemaAndTable := strings.Split(table, ".")
		rows := rc.query(
			`SELECT column_name, data_type, udt_name FROM information_schema.columns
WHERE table_schema = $1 AND table_name = $2
ORDER BY ordinal_position`,
			schemaAndTable[0],
			schemaAndTable[1],
		)

		var columnName, dataType, udtName string
		for rows.Next() {
			rows.Scan(&columnName, &dataType, &udtName)

			if dataType == "ARRAY" {
				udtName = fmt.Sprintf("%s[]", udtName[1:])
			}

			rc.columnsCache[table] = append(
				rc.columnsCache[table],
				db.Column{Name: columnName, Type: pgTypeToDbType(udtName)},
			)
		}
	}

	newTables := rc.setupPublication()
	currentLSN, backfillLSN, snapshotName := rc.setupReplicationSlot(len(newTables) > 0)

	log.Println("Current LSN:", currentLSN, "Backfill LSN:", backfillLSN, "Snapshot name:", snapshotName)

	return newTables, uint64(backfillLSN), snapshotName
}

func (rc *ReplicationClient) Start(startPosition uint64, endPosition uint64) chan *db.Transaction {
	if rc.running {
		log.Fatalln("Replication is already running")
	}

	startLSN := pglogrepl.LSN(startPosition)
	endLSN := pglogrepl.LSN(endPosition)
	log.Println("Replicating startLSN:", startPosition, startLSN)
	log.Println("Replicating endLSN:", endPosition, endLSN)

	conn := rc.streamConn.PgConn()

	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", rc.publicationName),
		"streaming 'true'",
	}
	err := pglogrepl.StartReplication(
		context.Background(),
		conn,
		rc.publicationName,
		startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	)
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", rc.publicationName)

	transactions := make(chan *db.Transaction)
	rc.running = true

	go func() {
		log.Println("Goroutine started to read from replication stream")

		typeMap := pgtype.NewMap()
		inStream := false
		relations := map[uint32]*pglogrepl.RelationMessageV2{}
		clientXLogPos := startLSN
		standbyMessageTimeout := time.Second * 10
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
		var transaction *db.Transaction

	Out:
		for {
			select {
			case <-rc.done:
				log.Println("Received done signal. Stopping replication...")
				rc.streamConn.Close(context.Background())
				break Out
			default:
				// keep running
			}

			if time.Now().After(nextStandbyMessageDeadline) {
				var confirmLSN pglogrepl.LSN

				if rc.processingLSN == rc.lastProcessedLSN && rc.processingLSN > 0 {
					// we're up to date so we can move the replication slot forward freely
					confirmLSN = clientXLogPos
				} else {
					// we still haven't finished writing some stuff, so let's move the replication slot only up to the latest confirmed write
					confirmLSN = rc.lastProcessedLSN
				}

				err = pglogrepl.SendStandbyStatusUpdate(
					context.Background(),
					conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: confirmLSN},
				)
				if err != nil {
					log.Fatalln("SendStandbyStatusUpdate failed:", err)
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
			rawMsg, err := conn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if !rc.running {
					log.Println("No longer running. Stopping replication...")
					break Out
				}

				if pgconn.Timeout(err) {
					continue
				}
				log.Fatalln("ReceiveMessage failed:", err)
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				log.Fatalf("received Postgres WAL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				// log.Printf("Received unexpected message: %T\n", rawMsg)
				// This is fine... Usually happens when a trigger or other plsql code sends a NOTICE.
				// Let's not print anything to avoid log spam.
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}

				if transaction == nil {
					transaction = &db.Transaction{
						StreamPosition: uint64(xld.WALStart),
						Changes:        make(chan *db.Change, 3),
					}
					transactions <- transaction
				}

				change, done := processV2(xld.WALData, &inStream, relations, typeMap)
				transaction.Changes <- change
				if done {
					close(transaction.Changes)
					transaction = nil
				}
				rc.processingLSN = xld.WALStart

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				if endLSN != 0 && xld.WALStart >= endLSN {
					log.Println("Reached end LSN. Stopping replication...")
					rc.running = false
					rc.ResetStreamConn()
					close(transactions)
					return
				}
			}
		}

		log.Println("Replication stream exiting...")
		rc.streamConn.Close(context.Background())
		close(transactions)
		rc.Close()
	}()

	return transactions
}

func (rc *ReplicationClient) SetProcessedLSN(lsn uint64) {
	rc.lastProcessedLSN = pglogrepl.LSN(lsn)
}

func (rc *ReplicationClient) Close() {
	select {
	case <-rc.done:
	default:
		close(rc.done)
	}

	rc.running = false
	rc.streamConn.Close(context.Background())
	rc.conn.Close(context.Background())
}

func (rc *ReplicationClient) setupPublication() []string {
	var pubCount int
	row := rc.query1(
		"select count(*) from pg_publication where pubname = $1",
		rc.publicationName)

	err := row.Scan(&pubCount)
	if err != nil {
		log.Fatalf("Query \"select count(*) from pg_publication where pubname = $1\" failed: %v\n", err)
	}

	if pubCount < 1 {
		rc.exec(fmt.Sprintf("create publication \"%s\" with (publish_via_partition_root = true)", rc.publicationName))
	}

	rows := rc.query(
		"select schemaname || '.' || tablename from pg_publication_tables where pubname = $1",
		rc.publicationName)
	defer rows.Close()

	var table string
	publishedTables := make(map[string]bool)
	for rows.Next() {
		rows.Scan(&table)
		publishedTables[table] = true
	}

	configuredTables := make(map[string]bool)
	for _, table = range rc.tables {
		configuredTables[table] = true
	}

	tablesToUnpublish := make([]string, 0)
	for table := range publishedTables {
		if !configuredTables[table] {
			tablesToUnpublish = append(tablesToUnpublish, table)
		}
	}

	if len(tablesToUnpublish) > 0 {
		rc.exec(fmt.Sprintf(
			"alter publication \"%s\" drop table %s",
			rc.publicationName,
			strings.Join(tablesToUnpublish, ",")))
	}

	tablesToPublish := make([]string, 0)
	for table := range configuredTables {
		if !publishedTables[table] {
			tablesToPublish = append(tablesToPublish, table)
		}
	}

	// FIXME: Things will go to shit if a backfill fails midway through...
	//        We should actually only add the tables to the publication after the
	//        backfill is done, and right before starting to stream
	if len(tablesToPublish) > 0 {
		rc.exec(fmt.Sprintf(
			"alter publication \"%s\" add table %s;",
			rc.publicationName,
			strings.Join(tablesToPublish, ",")))
	}

	log.Println("Publication is set-up:", rc.publicationName)
	return tablesToPublish
}

func (rc *ReplicationClient) setupReplicationSlot(createBackfillSnapshot bool) (pglogrepl.LSN, pglogrepl.LSN, string) {
	row := rc.query1(
		"select count(*) from pg_replication_slots where slot_name = $1 and database = $2;",
		rc.publicationName,
		rc.connCfg.Database)

	var slotCount int
	err := row.Scan(&slotCount)
	if err != nil {
		log.Fatalf("Query \"select count(*) from pg_replication_slots where slot_name = $1 and database = $2;\" failed: %v\n", err)
	}

	if slotCount > 1 {
		log.Fatalf("More than one replication slot with name %s found", rc.publicationName)
	}

	var snapshotName string
	var currentLSN pglogrepl.LSN
	var backfillLSN pglogrepl.LSN

	if slotCount < 1 {
		log.Println("Replication slot doesn't exist yet. Creating...")
		backfillLSN = rc.identifySystem().XLogPos
		snapshotName = rc.createReplicationSlot(false)
	} else if createBackfillSnapshot {
		log.Println("Replication slot already exists. Creating temporary slot for backfill...")
		backfillLSN = rc.identifySystem().XLogPos
		snapshotName = rc.createReplicationSlot(true)
	} else {
		log.Println("Replication slot already exists and no backfill needed... Getting current LSN")
		row := rc.query1(
			"select restart_lsn from pg_replication_slots where slot_name = $1 and database = $2;",
			rc.publicationName,
			rc.connCfg.Database)

		err := row.Scan(&currentLSN)
		if err != nil {
			log.Fatalf("Query \"select restart_lsn from pg_replication_slots where slot_name = $1 and database = $2;\" failed: %v\n", err)
		}
	}

	log.Println("Replication slot is up:", rc.publicationName)
	return currentLSN, backfillLSN, snapshotName
}

func (rc *ReplicationClient) identifySystem() pglogrepl.IdentifySystemResult {
	sysident, err := pglogrepl.IdentifySystem(context.Background(), rc.streamConn.PgConn())
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	return sysident
}

func (rc *ReplicationClient) createReplicationSlot(temporary bool) string {
	slotName := rc.publicationName
	if temporary {
		slotName = fmt.Sprintf("%s_temp", rc.publicationName)

		var pid int
		rc.query1(
			"select active_pid from pg_replication_slots where slot_name = $1",
			slotName,
		).Scan(&pid)

		if pid > 0 {
			// FIXME: This is a hack and we should be using advisory locks instead
			rc.exec("select pg_terminate_backend($1)", pid)
		}

		pglogrepl.DropReplicationSlot(
			context.Background(),
			rc.streamConn.PgConn(),
			slotName,
			pglogrepl.DropReplicationSlotOptions{Wait: true},
		)
	}

	result, err := pglogrepl.CreateReplicationSlot(
		context.Background(),
		rc.streamConn.PgConn(),
		fmt.Sprintf("\"%s\"", slotName),
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      temporary,
			SnapshotAction: "EXPORT_SNAPSHOT",
		})

	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}

	return result.SnapshotName
}

func (rc *ReplicationClient) ResetStreamConn() {
	rc.streamConn.Close(context.Background())
	rc.streamConn = rc.connect(true)
}

func (rc *ReplicationClient) connect(replication bool) *pgx.Conn {
	port := rc.connCfg.Port
	if port == 0 {
		port = 5432
	}

	params := make([]string, 0, 0)
	if replication {
		params = append(params, "replication=database")
	}

	if rc.connCfg.Ssl != "" {
		params = append(params, fmt.Sprintf("sslmode=%s", rc.connCfg.Ssl))
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s%s",
		url.QueryEscape(rc.connCfg.User),
		url.QueryEscape(rc.connCfg.Pass),
		url.QueryEscape(rc.connCfg.Host),
		port,
		url.QueryEscape(rc.connCfg.Database),
		params)

	config, err := pgx.ParseConfig(connString)
	if err != nil {
		log.Fatalln("Unable to parse connection string:", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalln("Unable to connect to postgres server:", err)
	}

	return conn
}

func (rc *ReplicationClient) query(sql string, values ...any) pgx.Rows {
	rows, err := rc.conn.Query(context.Background(), sql, values...)
	if err != nil {
		log.Fatalf("Query \"%s\" failed: %v\n", sql, err)
	}

	return rows
}

func (rc *ReplicationClient) query1(sql string, values ...any) pgx.Row {
	return rc.conn.QueryRow(context.Background(), sql, values...)
}

func (rc *ReplicationClient) exec(sql string, values ...any) {
	rc.query(sql, values...).Close()
}

// space, single quote, comma, period, asterisk need to be escaped with \
// https://github.com/eulerto/wal2json/blob/master/README.md#parameters
func escapeTableName(table string) string {
	return strings.NewReplacer(" ", `\ `, "'", `\'`, ",", `\,`, "*", `\*`).Replace(table)
}
