package truck

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type ExitMsg struct {
	TruckName string
	Msg       string
}

type Truck struct {
	Name string
	InputWriteConn db.ConnectionPool
	InputReadConn db.ConnectionPool
	InputTable string
	InputSql string
	OutputConn  db.ConnectionPool
	OutputTable string
	OutputSql string
	KillChan chan any
	ExitedChan chan ExitMsg
}

func NewTruck(cfg config.Truck, dbConnections map[string]db.Db, killChan chan any, exitedChan chan ExitMsg) Truck {
	return Truck{
		Name: cfg.Name,
		InputWriteConn: dbConnections[cfg.Input.Connection].Write,
		InputReadConn: dbConnections[cfg.Input.Connection].Read,
		InputTable: cfg.Input.Table,
		InputSql: cfg.Input.Sql,
		OutputConn: dbConnections[cfg.Output.Connection].Write,
		OutputTable: cfg.Output.Table,
		OutputSql: cfg.Output.Sql,
		KillChan: killChan,
		ExitedChan: exitedChan,
	}
}

// Startup:
// - connect to writer and setup replication slot if necessary
//
// - if we just created a new replication slot, we need to backfill the data (how?)
// for lines in input table {
//   - read line from input db
//   - write line to output db
//   - check if we received a kill message
//     - if we did, remove replication slot, truncate table, and exit
// }
//
// - launch a goroutine that reads from a channel and writes to the output db
//   - whenever we write something we need to update the replication slot's LSN
// - start reading from replication slot and writing to the channel
func (t *Truck) Start() {
	fmt.Printf("[Truck %s] Starting...\n", t.Name)
	defer func() {
		t.ExitedChan <- ExitMsg{t.Name, "Exited!"}
	}()

	t.setupReplication()

	for {
		select {
		case <-t.KillChan:
			fmt.Printf("[Truck %s] Received kill msg. Exiting...\n", t.Name)
			return
		}
	}

	// fmt.Println("Stopping truck", cfg.Name)
}

func (t *Truck) setupReplication() {
	const outputPlugin = "wal2json"

	publicationName := fmt.Sprintf("trucker_%s", t.Name)
	sql := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", publicationName, t.InputTable)
	result, err := t.InputWriteConn.Query(sql)
	if err != nil {
		log.Fatalln("[Truck %s] Error creating publication:", err)
	}
	fmt.Printf("Create publication result: %v", result)

	physicalConn, err := t.InputWriteConn.ConcretePool().(*pgxpool.Pool).Acquire(context.Background())
	if err != nil {
		log.Fatalln("[Truck %s] Error acquiring connection from pool:", err)
	}
	defer physicalConn.Release()

	pgConn := physicalConn.Conn().PgConn()
	sysident, err := pglogrepl.IdentifySystem(context.Background(), pgConn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := publicationName
	_, err = pglogrepl.CreateReplicationSlot(
		context.Background(),
		pgConn,
		slotName,
		outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: true},
	)
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)

	err = pglogrepl.StartReplication(
		context.Background(),
		pgConn,
		slotName,
		sysident.XLogPos,
		pglogrepl.StartReplicationOptions{},
	)
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), pgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		rawMsg, err := pgConn.ReceiveMessage(ctx)
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

			log.Printf("wal2json data: %s\n", string(xld.WALData))

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}
