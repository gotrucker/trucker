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

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type ReplicationClient struct {
	publicationName string
	tables          []string
	connCfg         config.Connection
	conn            *pgx.Conn
	streamConn      *pgx.Conn
	writtenLSN      pglogrepl.LSN
	running         bool
	done            chan bool
	columnsCache    map[string][]db.Column
}

func NewReplicationClient(tables []string, connCfg config.Connection) *ReplicationClient {
	return &ReplicationClient{
		publicationName: fmt.Sprintf("trucker_%s", connCfg.Database),
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

	err := pglogrepl.StartReplication(
		context.Background(),
		conn,
		rc.publicationName,
		startLSN,
		pglogrepl.StartReplicationOptions{},
	)
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", rc.publicationName)

	changes := make(chan *db.Transaction)
	rc.running = true

	go func() {
		log.Println("Goroutine started to read from replication stream")

		clientXLogPos := startLSN
		standbyMessageTimeout := time.Second * 10
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

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
				err = pglogrepl.SendStandbyStatusUpdate(
					context.Background(),
					conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: rc.writtenLSN},
				)
				if err != nil {
					log.Fatalln("SendStandbyStatusUpdate failed:", err)
				}
				// log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
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
				log.Printf("Received unexpected message: %T\n", rawMsg)
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

				changes <- &db.Transaction{
					Position:   uint64(xld.WALStart),
					Changesets: makeChangesets(xld.WALData, rc.columnsCache),
				}

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				if endLSN != 0 && xld.WALStart >= endLSN {
					log.Println("Reached end LSN. Stopping replication...")
					rc.running = false
					rc.ResetStreamConn()
					close(changes)
					return
				}
			}
		}

		log.Println("Replication stream exiting...")
		rc.streamConn.Close(context.Background())
		close(changes)
		rc.Close()
	}()

	return changes
}

func (rc *ReplicationClient) SetWrittenLSN(lsn uint64) {
	rc.writtenLSN = pglogrepl.LSN(lsn)
}

func (rc *ReplicationClient) Close() {
	select {
	case <-rc.done:
	default:
		close(rc.done)
	}

	rc.running = false
	
	// Only close connections if they exist
	if rc.streamConn != nil {
		rc.streamConn.Close(context.Background())
	}
	
	if rc.conn != nil {
		rc.conn.Close(context.Background())
	}
	
	// Clean up temporary replication slot if this is a backfill client
	if strings.Contains(rc.publicationName, "_backfill_") {
		// Connect to drop the slot
		tmpConn := rc.connect(true)
		defer tmpConn.Close(context.Background())
		
		// Drop the temporary replication slot
		pglogrepl.DropReplicationSlot(
			context.Background(),
			tmpConn.PgConn(),
			rc.publicationName,
			pglogrepl.DropReplicationSlotOptions{Wait: true},
		)
		
		log.Printf("Dropped temporary replication slot: %s\n", rc.publicationName)
	}
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

	// FIXME: This will go to shit if a backfill fails midway through...
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
		log.Println("Replication slot doesn't exit yet. Creating...")
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

// CreateBackfillClient creates a standalone replication client specifically for backfilling
// that won't interfere with streaming operations
func (rc *ReplicationClient) CreateBackfillClient() (*ReplicationClient, string, uint64) {
	// Create a new replication client with the same configuration
	backfillRC := &ReplicationClient{
		publicationName: fmt.Sprintf("%s_backfill_%d", rc.publicationName, time.Now().Unix()),
		tables:          rc.tables,
		connCfg:         rc.connCfg,
		running:         false,
		done:            make(chan bool, 1),
		columnsCache:    rc.columnsCache, // Share the existing columns cache
	}
	
	// Connect to the database
	backfillRC.conn = backfillRC.connect(false)
	backfillRC.streamConn = backfillRC.connect(true)
	
	// Create a temporary replication slot and get snapshot name
	backfillLSN := backfillRC.identifySystem().XLogPos
	snapshotName := backfillRC.createReplicationSlot(true)
	
	return backfillRC, snapshotName, uint64(backfillLSN)
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
		"wal2json",
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

	replicationParam := ""
	if replication {
		replicationParam = "?replication=database"
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s%s",
		url.QueryEscape(rc.connCfg.User),
		url.QueryEscape(rc.connCfg.Pass),
		url.QueryEscape(rc.connCfg.Host),
		port,
		url.QueryEscape(rc.connCfg.Database),
		replicationParam)

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
