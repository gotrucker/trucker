package truck

import (
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/pg"
)

type ExitMsg struct {
	TruckName string
	Msg       string
}

type Truck struct {
	Name              string
	ReplicationClient *pg.ReplicationClient
	Reader            *pg.Reader
	InputTable        string
	Writer            *pg.Writer
	OutputTable       string
	OutputSql         string
	ChangesChan chan *pg.Changeset
	KillChan          chan any
	DoneChan          chan ExitMsg
}

func NewTruck(cfg config.Truck, rc *pg.ReplicationClient, dbConnections map[string]*pgx.Conn, doneChan chan ExitMsg) Truck {
	return Truck{
		Name:              cfg.Name,
		ReplicationClient: rc,
		Reader:            pg.NewReader(cfg.Input.Sql, dbConnections[cfg.Input.Connection]),
		InputTable:        cfg.Input.Table,
		Writer:            pg.NewWriter(cfg.Input.Connection, cfg.Output.Sql, dbConnections[cfg.Output.Connection]),
		OutputTable:       cfg.Output.Table,
		ChangesChan: make(chan *pg.Changeset),
		KillChan:          make(chan any),
		DoneChan:          doneChan,
	}
}

func (t *Truck) Backfill(snapshotName string, targetLSN int64) {
	log.Printf("[Truck %s] Running backfill...\n", t.Name)
	t.Writer.TruncateTable(t.OutputTable)
	backfillChan := t.ReplicationClient.StreamBackfillData(t.InputTable, snapshotName)

	for {
		backfillBatch := <-backfillChan
		if backfillBatch == nil || len(backfillBatch.Rows) == 0 {
			if t.Writer.GetCurrentLsn() == 0 {
				t.Writer.SetupLsnTracking()
				t.Writer.SetCurrentLsn(targetLSN)
			}

			log.Printf("[Truck %s] Backfill complete!\n", t.Name)
			break
		}

		cols, rows := t.Reader.Read("insert", backfillBatch.Columns, backfillBatch.Rows)
		t.Writer.Write(cols, rows)
	}
}

func (t *Truck) Start() {
	log.Printf("[Truck %s] Starting to read from replication stream...\n", t.Name)
	defer func() {
		t.DoneChan <- ExitMsg{t.Name, "Exited!"}
	}()

	go func() {
		for {
			select {
			case <-t.KillChan:
				log.Printf("[Truck %s] Received kill msg. Exiting...\n", t.Name)
				close(t.KillChan)
				close(t.ChangesChan)
				return
			case changeset := <-t.ChangesChan:
				if changeset == nil {
					log.Printf("[Truck %s] Changeset channel closed. Exiting...\n", t.Name)
					return
				} else {
					insertCols, insertVals := t.Reader.Read("insert", changeset.InsertColumns, changeset.InsertValues)
					updateCols, updateVals := t.Reader.Read("update", changeset.UpdateColumns, changeset.UpdateValues)
					deleteCols, deleteVals := t.Reader.Read("delete", changeset.DeleteColumns, changeset.DeleteValues)

					t.Writer.WithTransaction(func() {
						fmt.Println("Inserting:", insertCols, insertVals)
						t.Writer.Write(insertCols, insertVals)
						fmt.Println("Updating:", updateCols, updateVals)
						t.Writer.Write(updateCols, updateVals)
						fmt.Println("Deleting:", deleteCols, deleteVals)
						t.Writer.Write(deleteCols, deleteVals)
					})
				}
			}
		}
	}()
}

func (t *Truck) ProcessChangeset(changeset *pg.Changeset) {
	t.ChangesChan <- changeset
}

func (t *Truck) Stop() {
	select {
	case <-t.KillChan:
	default:
		close(t.KillChan)
	}
}
