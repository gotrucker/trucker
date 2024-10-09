package pg

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

type ReplicationClient struct {
	publicationName string
	tables [][]string
	connCfg config.Connection
	conn *pgx.Conn
	streamConn *pgx.Conn
	running bool
	runningMutex sync.Mutex
}

func NewReplicationClient(tables [][]string, connCfg config.Connection) *ReplicationClient {
	return &ReplicationClient{
		publicationName: fmt.Sprintf("%s_%s", "trucker", connCfg.Database),
		tables: tables,
		connCfg: connCfg,
		running: false,
		runningMutex: sync.Mutex{},
	}
}

func (client *ReplicationClient) Setup() ([]string, pglogrepl.LSN, string) {
	client.conn = client.connect(false, false)
	client.streamConn = client.connect(true, false)
	defer client.streamConn.Close(context.Background())

	newTables := client.setupPublication()
	currentLSN, backfillLSN, snapshotName := client.setupReplicationSlot(len(newTables) > 0)

	log.Println("Current LSN:", currentLSN, "Backfill LSN:", backfillLSN, "Snapshot name:", snapshotName)

	return newTables, backfillLSN, snapshotName
}

func (client *ReplicationClient) Backfill(table string, snapshotName string) {
	conn := client.connect(false, true)
	conn.Exec(context.Background(), "begin transaction isolation level repeatable read")
	conn.Exec(context.Background(), fmt.Sprintf("set transaction snapshot '%s'", snapshotName))

	// https://www.percona.com/blog/working-with-snapshots-in-postgresql/
	rows, err := conn.Query(context.Background(), fmt.Sprintf("select id, name from %s order by id", table))
	if err != nil {
		log.Fatalf("Query failed: %v\n", err)
	}

	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		if err != nil {
			log.Fatalf("Scan failed: %v\n", err)
		}

		log.Printf("id: %d, name: %s\n", id, name)
	}
}

func (client *ReplicationClient) Start() chan []map[string]any {
	if client.isRunning() {
		log.Fatalln("Replication is already running")
	}

	client.runningMutex.Lock()
	client.running = true
	client.runningMutex.Unlock()

	channel := make(chan []map[string]any)

	err := pglogrepl.StartReplication(
		context.Background(),
		client.streamConn.PgConn(),
		client.publicationName,
		pglogrepl.LSN(1), // TODO FIXME // sysident.XLogPos,
		pglogrepl.StartReplicationOptions{},
	)
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}

	log.Println("Logical replication started on slot", client.publicationName)

	// TODO launch a goroutine that:
	// - while client.isRunning() {
	//   - listen on replication slot
	//   - publish messages on channel
	//   }

	return channel
}

func (client *ReplicationClient) Stop() {
	client.runningMutex.Lock()
	client.running = false
	client.runningMutex.Unlock()
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
		"select schemaname, tablename from pg_publication_tables where pubname = $1",
		client.publicationName)

	var schemaname, tablename string
	publishedTables := make(map[string]bool)
	for rows.Next() {
		rows.Scan(&schemaname, &tablename)
		table := fmt.Sprintf("\"%s\".\"%s\"", schemaname, tablename)
		publishedTables[table] = true
	}

	configuredTables := make(map[string]bool)
	for _, entry := range client.tables {
		table := fmt.Sprintf("\"%s\".\"%s\"", entry[0], entry[1])
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
		backfillLSN = client.identifySystem().XLogPos
		snapshotName = client.createReplicationSlot(false)
	} else if createBackfillSnapshot {
		backfillLSN = client.identifySystem().XLogPos
		snapshotName = client.createReplicationSlot(true)
	} else {
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
	}

	result, err := pglogrepl.CreateReplicationSlot(
		context.Background(),
		client.streamConn.PgConn(),
		fmt.Sprintf("\"%s\"", slotName),
		"wal2json",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: temporary,
			SnapshotAction: "EXPORT_SNAPSHOT",
		})

	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}

	return result.SnapshotName
}

func (client *ReplicationClient) connect(replication bool, replica bool) *pgx.Conn {
	replicationStr := ""
	host := client.connCfg.Host
	port := client.connCfg.Port

	if replication {
		replicationStr = "?replication=database"
	}

	if replica && client.connCfg.ReplicaHost != "" {
		host = client.connCfg.ReplicaHost

		if client.connCfg.ReplicaPort != 0 {
			port = client.connCfg.ReplicaPort
		} else {
			port = 5432
		}
	}

	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s%s",
		client.connCfg.User,
		client.connCfg.Pass,
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
	rows := client.query(sql, values...)
	for rows.Next() {}
}

func (client *ReplicationClient) isRunning() bool {
	client.runningMutex.Lock()
	running := client.running
	client.runningMutex.Unlock()

	return running
}
