package postgres

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/tonyfg/trucker/pkg/config"
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
}

func NewReplicationClient(tables []string, connCfg config.Connection) *ReplicationClient {
	return &ReplicationClient{
		publicationName: fmt.Sprintf("%s_%s", "trucker", connCfg.Database),
		tables:          tables,
		connCfg:         connCfg,
		running:         false,
		done:            make(chan bool, 1),
	}
}

func (rc *ReplicationClient) Setup() ([]string, uint64, string) {
	rc.conn = rc.connect(false)
	rc.streamConn = rc.connect(true)
	// we need to keep the connection open so that the other connection can use
	// the repliaction slot snapshot for backfills
	// defer client.streamConn.Close(context.Background())

	newTables := rc.setupPublication()
	currentLSN, backfillLSN, snapshotName := rc.setupReplicationSlot(len(newTables) > 0)

	log.Println("Current LSN:", currentLSN, "Backfill LSN:", backfillLSN, "Snapshot name:", snapshotName)

	return newTables, uint64(backfillLSN), snapshotName
}

func (rc *ReplicationClient) Start(startPosition uint64, endPosition uint64) chan map[string]*Changeset {
	if rc.running {
		log.Fatalln("Replication is already running")
	}

	startLSN := pglogrepl.LSN(startPosition)
	endLSN := pglogrepl.LSN(endPosition)
	fmt.Println("Replicating startLSN:", startPosition, startLSN)
	fmt.Println("Replicating endLSN:", endPosition, endLSN)

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

	changes := make(chan map[string]*Changeset)
	rc.running = true

	go func() {
		log.Println("Goroutine started to read from replication stream")

		clientXLogPos := startLSN
		standbyMessageTimeout := time.Second * 10
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

		fmt.Printf("Starting loop! %v\n", rc.running)

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
				log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
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
				log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
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

				changes <- makeChangesets(xld.WALData)

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

		fmt.Println("Replication stream exiting...")
		rc.streamConn.Close(context.Background())
		close(changes)
		rc.Close()
	}()

	return changes
}

func (rc *ReplicationClient) SetWrittenLSN(lsn pglogrepl.LSN) {
	rc.writtenLSN = lsn
}

func (rc *ReplicationClient) Close() {
	select {
	case <-rc.done:
	default:
		close(rc.done)
	}

	rc.running = false
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
		rc.exec(fmt.Sprintf("create publication \"%s\"", rc.publicationName))
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
		fmt.Println("Replication slot doesn't exit yet. Creating...")
		backfillLSN = rc.identifySystem().XLogPos
		snapshotName = rc.createReplicationSlot(false)
	} else if createBackfillSnapshot {
		fmt.Println("Replication slot already exists. Creating temporary slot for backfill...")
		backfillLSN = rc.identifySystem().XLogPos
		snapshotName = rc.createReplicationSlot(true)
	} else {
		fmt.Println("Replication slot already exists and no backfill needed... Getting current LSN")
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
	return NewConnection(
		rc.connCfg.User,
		rc.connCfg.Pass,
		rc.connCfg.Host,
		rc.connCfg.Port,
		rc.connCfg.Database,
		replication,
	)
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
