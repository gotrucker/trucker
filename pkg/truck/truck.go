package truck

import (
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
)

type ExitMsg struct {
	TruckName string
	Msg       string
}

type Truck struct {
	Name           string
	InputWriteConn *pgx.Conn
	InputReadConn  *pgx.Conn
	InputTable     string
	InputSql       string
	OutputConn     *pgx.Conn
	OutputSql      string
	KillChan       chan any
	ExitedChan     chan ExitMsg
}

func NewTruck(cfg config.Truck, dbConnections map[string]*pgx.Conn, killChan chan any, exitedChan chan ExitMsg) Truck {
	return Truck{
		Name:           cfg.Name,
		InputWriteConn: dbConnections[cfg.Input.Connection],
		InputReadConn:  dbConnections[cfg.Input.Connection], // FIXME: need separate read/write connections
		InputTable:     cfg.Input.Table,
		InputSql:       cfg.Input.Sql,
		OutputConn:     dbConnections[cfg.Output.Connection],
		OutputSql:      cfg.Output.Sql,
		KillChan:       killChan,
		ExitedChan:     exitedChan,
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
//   - if we did, remove replication slot, truncate table, and exit
//     }
//
// - launch a goroutine that reads from a channel and writes to the output db
//   - whenever we write something we need to update the replication slot's LSN
//
// - start reading from replication slot and writing to the channel
func (t *Truck) Start() {
	fmt.Printf("[Truck %s] Starting...\n", t.Name)
	defer func() {
		t.ExitedChan <- ExitMsg{t.Name, "Exited!"}
	}()

	for {
		select {
		case <-t.KillChan:
			fmt.Printf("[Truck %s] Received kill msg. Exiting...\n", t.Name)
			return
		}
	}
}
