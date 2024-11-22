package pg

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
)

type ReplicationClient struct {
	publicationName string
	tables          []string
	connCfg         config.Connection
	conn            *pgx.Conn
	streamConn      *pgx.Conn
	running         bool
	done            chan bool
}

type TableChanges struct {
	InsertSql    *strings.Builder
	InsertCols   []string
	InsertValues []sqlValue
	UpdateSql    *strings.Builder
	UpdateCols   []string
	UpdateValues []sqlValue
	DeleteSql    *strings.Builder
	DeleteCols   []string
	DeleteValues []sqlValue
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

func (client *ReplicationClient) Setup() ([]string, pglogrepl.LSN, string) {
	client.conn = client.connect(false)
	client.streamConn = client.connect(true)
	// we need to keep the connection open so that the other connection can use
	// the repliaction slot snapshot for backfills
	// defer client.streamConn.Close(context.Background())

	newTables := client.setupPublication()
	currentLSN, backfillLSN, snapshotName := client.setupReplicationSlot(len(newTables) > 0)

	log.Println("Current LSN:", currentLSN, "Backfill LSN:", backfillLSN, "Snapshot name:", snapshotName)

	return newTables, backfillLSN, snapshotName
}

func (client *ReplicationClient) Start(startLSN pglogrepl.LSN) chan map[string]*TableChanges {
	if client.running {
		log.Fatalln("Replication is already running")
	}

	conn := client.streamConn.PgConn()

	err := pglogrepl.StartReplication(
		context.Background(),
		conn,
		client.publicationName,
		startLSN,
		pglogrepl.StartReplicationOptions{},
	)
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", client.publicationName)

	changes := make(chan map[string]*TableChanges)
	client.running = true

	go func() {
		log.Println("Goroutine started to read from replication stream")

		clientXLogPos := startLSN
		standbyMessageTimeout := time.Second * 10
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
		// relations := map[uint32]*pglogrepl.RelationMessage{}
		// relationsV2 := map[uint32]*pglogrepl.RelationMessageV2{}
		// typeMap := pgtype.NewMap()

		fmt.Printf("Starting loop! %v\n", client.running)

	Out:
		for {
			select {
			case <-client.done:
				log.Println("Received done signal. Stopping replication...")
				client.running = false
				client.streamConn.Close(context.Background())
				break Out
			default:
				// keep running
			}

			if time.Now().After(nextStandbyMessageDeadline) {
				err = pglogrepl.SendStandbyStatusUpdate(
					context.Background(),
					conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos},
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

				log.Printf("wal2json tx LSN: %s / %s\n", xld.WALStart, xld.ServerWALEnd)
				log.Printf("wal2json data: %s\n", xld.WALData)
				changes <- wal2jsonToSqlValues(xld.WALData)

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}
			}
		}

		fmt.Println("Replication stream exiting...")
	}()

	return changes
}

func (client *ReplicationClient) Stop() {
	close(client.done)
	client.conn.Close(context.Background())
}

func (client *ReplicationClient) setupPublication() []string {
	var pubCount int
	row := client.query1(
		"select count(*) from pg_publication where pubname = $1",
		client.publicationName)

	err := row.Scan(&pubCount)
	if err != nil {
		log.Fatalf("Query \"select count(*) from pg_publication where pubname = $1\" failed: %v\n", err)
	}

	if pubCount < 1 {
		client.exec(fmt.Sprintf("create publication \"%s\"", client.publicationName))
	}

	rows := client.query(
		"select schemaname || '.' || tablename from pg_publication_tables where pubname = $1",
		client.publicationName)
	defer rows.Close()

	var table string
	publishedTables := make(map[string]bool)
	for rows.Next() {
		rows.Scan(&table)
		publishedTables[table] = true
	}

	configuredTables := make(map[string]bool)
	for _, table = range client.tables {
		configuredTables[table] = true
	}

	tablesToUnpublish := make([]string, 0)
	for table := range publishedTables {
		if !configuredTables[table] {
			tablesToUnpublish = append(tablesToUnpublish, table)
		}
	}

	if len(tablesToUnpublish) > 0 {
		client.exec(fmt.Sprintf(
			"alter publication \"%s\" drop table %s",
			client.publicationName,
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
		client.exec(fmt.Sprintf(
			"alter publication \"%s\" add table %s;",
			client.publicationName,
			strings.Join(tablesToPublish, ",")))
	}

	log.Println("Publication is set-up:", client.publicationName)
	return tablesToPublish
}

func (client *ReplicationClient) setupReplicationSlot(createBackfillSnapshot bool) (pglogrepl.LSN, pglogrepl.LSN, string) {
	row := client.query1(
		"select count(*) from pg_replication_slots where slot_name = $1 and database = $2;",
		client.publicationName,
		client.connCfg.Database)

	var slotCount int
	err := row.Scan(&slotCount)
	if err != nil {
		log.Fatalf("Query \"select count(*) from pg_replication_slots where slot_name = $1 and database = $2;\" failed: %v\n", err)
	}

	if slotCount > 1 {
		log.Fatalf("More than one replication slot with name %s found", client.publicationName)
	}

	var snapshotName string
	var currentLSN pglogrepl.LSN
	var backfillLSN pglogrepl.LSN

	if slotCount < 1 {
		fmt.Println("Replication slot doesn't exit yet. Creating...")
		backfillLSN = client.identifySystem().XLogPos
		snapshotName = client.createReplicationSlot(false)
	} else if createBackfillSnapshot {
		fmt.Println("Replication slot already exists. Creating temporary slot for backfill...")
		backfillLSN = client.identifySystem().XLogPos
		snapshotName = client.createReplicationSlot(true)
	} else {
		fmt.Println("Replication slot already exists and no backfill needed... Getting current LSN")
		row := client.query1(
			"select restart_lsn from pg_replication_slots where slot_name = $1 and database = $2;",
			client.publicationName,
			client.connCfg.Database)

		err := row.Scan(&currentLSN)
		if err != nil {
			log.Fatalf("Query \"select restart_lsn from pg_replication_slots where slot_name = $1 and database = $2;\" failed: %v\n", err)
		}
	}

	log.Println("Replication slot is up:", client.publicationName)
	return currentLSN, backfillLSN, snapshotName
}

func (client *ReplicationClient) identifySystem() pglogrepl.IdentifySystemResult {
	sysident, err := pglogrepl.IdentifySystem(context.Background(), client.streamConn.PgConn())
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	return sysident
}

func (client *ReplicationClient) createReplicationSlot(temporary bool) string {
	slotName := client.publicationName
	if temporary {
		slotName = fmt.Sprintf("%s_temp", client.publicationName)

		var pid int
		client.query1(
			"select active_pid from pg_replication_slots where slot_name = $1",
			slotName,
		).Scan(&pid)

		if pid > 0 {
			client.exec("select pg_terminate_backend($1)", pid)
		}

		pglogrepl.DropReplicationSlot(
			context.Background(),
			client.streamConn.PgConn(),
			slotName,
			pglogrepl.DropReplicationSlotOptions{Wait: true},
		)
	}

	result, err := pglogrepl.CreateReplicationSlot(
		context.Background(),
		client.streamConn.PgConn(),
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

func (client *ReplicationClient) connect(replication bool) *pgx.Conn {
	replicationStr := ""
	host := client.connCfg.Host
	port := client.connCfg.Port

	if replication {
		replicationStr = "?replication=database"
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s%s",
		client.connCfg.User,
		url.QueryEscape(client.connCfg.Pass),
		host,
		port,
		client.connCfg.Database,
		replicationStr)

	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}

	return conn
}

func (client *ReplicationClient) query(sql string, values ...any) pgx.Rows {
	rows, err := client.conn.Query(context.Background(), sql, values...)
	if err != nil {
		log.Fatalf("Query \"%s\" failed: %v\n", sql, err)
	}

	return rows
}

func (client *ReplicationClient) query1(sql string, values ...any) pgx.Row {
	return client.conn.QueryRow(context.Background(), sql, values...)
}

func (client *ReplicationClient) exec(sql string, values ...any) {
	client.query(sql, values...).Close()
}
