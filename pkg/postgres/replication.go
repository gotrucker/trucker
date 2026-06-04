package postgres

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

// Subscriber represents one truck that wants to receive transactions from a replication stream.
type Subscriber struct {
	Name     string
	Tables   map[string]bool
	Ch       chan db.Transaction
	StartLSN uint64     // initial LSN seeded into the per-truck ack map
	Done     <-chan any // closed when the truck is shutting down; nil means ignored
}

type ReplicationClient struct {
	publicationName string
	tables          []string
	connCfg         config.Connection
	conn            *pgx.Conn
	streamConn      *pgx.Conn
	running         bool
	done            chan struct{}
	doneChan        chan struct{}
	columnsCache    map[string][]db.Column

	mu          sync.Mutex
	truckLSNs   map[string]uint64
	minTruckLSN pglogrepl.LSN
	subscribers []Subscriber
}

func NewReplicationClient(tables []string, connCfg config.Connection, uniqueId string) *ReplicationClient {
	// doneChan is created pre-closed because the replication client hasn't been
	// started yet, so we avoid a deadlock in case someone call WaitDone() before
	// Start()
	preClosed := make(chan struct{})
	close(preClosed)
	return &ReplicationClient{
		publicationName: fmt.Sprintf("trucker_%s%s", connCfg.Database, uniqueId),
		tables:          tables,
		connCfg:         connCfg,
		running:         false,
		done:            make(chan struct{}),
		doneChan:        preClosed,
		columnsCache:    make(map[string][]db.Column),
		truckLSNs:       make(map[string]uint64),
		subscribers:     make([]Subscriber, 0),
	}
}

// Register adds a subscriber (truck) that will receive transactions from this replication stream.
// Must be called before Start.
func (rc *ReplicationClient) Register(sub Subscriber) {
	rc.subscribers = append(rc.subscribers, sub)
	rc.mu.Lock()
	rc.truckLSNs[sub.Name] = sub.StartLSN
	rc.recomputeMinLSN()
	rc.mu.Unlock()
}

// AckLSN is called by a truck after it has durably written up to lsn.
func (rc *ReplicationClient) AckLSN(name string, lsn uint64) {
	rc.mu.Lock()
	if lsn > rc.truckLSNs[name] {
		rc.truckLSNs[name] = lsn
		rc.recomputeMinLSN()
	}
	rc.mu.Unlock()
}

// AutoAdvance is called by the parser for subscribers that had no rows in a committed xid.
// It advances the in-memory ack without the truck writing anything durably.
func (rc *ReplicationClient) AutoAdvance(name string, lsn uint64) {
	rc.mu.Lock()
	if lsn > rc.truckLSNs[name] {
		rc.truckLSNs[name] = lsn
		rc.recomputeMinLSN()
	}
	rc.mu.Unlock()
}

// MinTruckLSN returns the minimum durably-acked LSN across all registered trucks.
func (rc *ReplicationClient) MinTruckLSN() uint64 {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return uint64(rc.minTruckLSN)
}

func (rc *ReplicationClient) recomputeMinLSN() {
	if len(rc.truckLSNs) == 0 {
		return
	}
	var min uint64 = ^uint64(0)
	for _, v := range rc.truckLSNs {
		if v < min {
			min = v
		}
	}
	rc.minTruckLSN = pglogrepl.LSN(min)
}

func (rc *ReplicationClient) Setup() ([]string, uint64, string) {
	rc.conn = rc.connect(false)
	rc.streamConn = rc.connect(true)

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

// Start begins streaming replication from startPosition. If endPosition is non-zero, the stream
// stops when startPosition >= endPosition. Subscribers receive transactions directly via their
// registered channels. Call WaitDone to block until the stream goroutine exits.
func (rc *ReplicationClient) Start(startPosition uint64, endPosition uint64) {
	if rc.running {
		log.Fatalln("Replication is already running")
	}
	select {
	case <-rc.done:
		return // already closed; skip starting a goroutine that would immediately exit
	default:
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

	rc.running = true
	rc.doneChan = make(chan struct{})

	go func() {
		parser := NewReplicationMessageParser(rc.subscribers, rc.AutoAdvance, rc.done)
		permanentShutdown := true // false when we exit via natural endLSN
		defer func() {
			parser.flushAll()
			if permanentShutdown {
				parser.closeSubscribers()
				rc.streamConn.Close(context.Background())
			}
			close(rc.doneChan)
			rc.running = false
		}()

		clientXLogPos := startLSN
		standbyMessageTimeout := time.Second * 10
		nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	Out:
		for {
			select {
			case <-rc.done:
				log.Println("Received done signal. Stopping replication...")
				break Out
			default:
			}

			if time.Now().After(nextStandbyMessageDeadline) {
				confirmLSN := rc.confirmLSN(clientXLogPos)
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
				select {
				case <-rc.done:
					log.Println("Replication stopping...")
					break Out
				default:
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
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
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

				parser.parseReplicationMsg(xld.WALData, uint64(xld.WALStart))

				if xld.WALStart > clientXLogPos {
					clientXLogPos = xld.WALStart
				}

				if endLSN != 0 && xld.WALStart >= endLSN {
					log.Println("Reached end LSN. Stopping replication...")
					permanentShutdown = false
					rc.ResetStreamConn()
					return
				}
			}
		}
	}()
}

// WaitDone returns a channel that is closed when the current streaming goroutine exits.
func (rc *ReplicationClient) WaitDone() <-chan struct{} {
	return rc.doneChan
}

func (rc *ReplicationClient) confirmLSN(clientXLogPos pglogrepl.LSN) pglogrepl.LSN {
	rc.mu.Lock()
	min := rc.minTruckLSN
	rc.mu.Unlock()

	if len(rc.subscribers) == 0 || min == 0 {
		return clientXLogPos
	}
	if min > clientXLogPos {
		return clientXLogPos
	}
	return min
}

func (rc *ReplicationClient) Close() {
	select {
	case <-rc.done:
	default:
		close(rc.done)
	}
	if rc.conn != nil {
		rc.conn.Close(context.Background())
	}
	if rc.streamConn != nil {
		rc.streamConn.Close(context.Background())
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
	connStr := connString(
		rc.connCfg.User,
		rc.connCfg.Pass,
		rc.connCfg.Host,
		rc.connCfg.Port,
		rc.connCfg.Database,
		rc.connCfg.Ssl,
		replication,
	)

	config, err := pgx.ParseConfig(connStr)
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
