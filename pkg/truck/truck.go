package truck

import (
	// "fmt"
	"log"
	"time"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
	"github.com/tonyfg/trucker/pkg/postgres"
)

type ExitMsg struct {
	TruckName string
	Msg       string
}

type Truck struct {
	Name              string
	ReplicationClient *postgres.ReplicationClient
	Reader            db.Reader
	InputTable        string
	Writer            db.Writer
	OutputTable       string
	OutputSql         string
	ChangesChan       chan *postgres.Changeset
	KillChan          chan any
	DoneChan          chan ExitMsg
	CurrentPosition   uint64
}

func NewTruck(cfg config.Truck, rc *postgres.ReplicationClient, connCfgs map[string]config.Connection, doneChan chan ExitMsg) Truck {
	return Truck{
		Name:              cfg.Name,
		ReplicationClient: rc,
		Reader:            db.NewReader(cfg.Input.Sql, connCfgs[cfg.Input.Connection]),
		InputTable:        cfg.Input.Table,
		Writer:            db.NewWriter(cfg.Input.Connection, cfg.Output.Sql, connCfgs[cfg.Output.Connection]),
		OutputTable:       cfg.Output.Table,
		ChangesChan:       make(chan *postgres.Changeset),
		KillChan:          make(chan any),
		DoneChan:          doneChan,
	}
}

func (t *Truck) Backfill(snapshotName string, targetLSN uint64) {
	start := time.Now()
	log.Printf("[Truck %s] Running backfill...\n", t.Name)
	backfillChan := t.ReplicationClient.StreamBackfillData(t.InputTable, snapshotName)

	for {
		backfillBatch := <-backfillChan
		if backfillBatch == nil || len(backfillBatch.Rows) == 0 {
			curPos := t.Writer.GetCurrentPosition()
			if curPos == 0 {
				t.Writer.SetupPositionTracking()
				t.Writer.SetCurrentPosition(targetLSN)
				t.CurrentPosition = targetLSN
			} else {
				t.CurrentPosition = curPos
			}

			log.Printf("[Truck %s] Backfill complete in %f seconds!\n", t.Name, time.Since(start).Seconds())
			break
		}

		cols, rows := t.Reader.Read("insert", backfillBatch.Columns, backfillBatch.Types, backfillBatch.Rows)
		log.Printf("Backfilling %d rows...\n", len(rows))
		if len(rows) > 0 {
			t.Writer.Write("insert", cols, rows)
		} else {
			log.Printf("Empty row batch after read query... Skipping\n")
		}
	}
}

func (t *Truck) Start() {
	log.Printf("[Truck %s] Starting to read from replication stream...\n", t.Name)

	go func() {
		defer func() {
			t.DoneChan <- ExitMsg{t.Name, "Exited!"}
		}()

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
					insertCols, insertVals := t.Reader.Read("insert", changeset.InsertColumns, changeset.InsertTypes, changeset.InsertValues)
					updateCols, updateVals := t.Reader.Read("update", changeset.UpdateColumns, changeset.UpdateTypes, changeset.UpdateValues)
					deleteCols, deleteVals := t.Reader.Read("delete", changeset.DeleteColumns, changeset.DeleteTypes, changeset.DeleteValues)

					t.Writer.WithTransaction(func() {
						t.Writer.Write("insert", insertCols, insertVals)
						t.Writer.Write("update", updateCols, updateVals)
						t.Writer.Write("delete", deleteCols, deleteVals)
					})
				}
			}
		}
	}()
}

func (t *Truck) ProcessChangeset(changeset *postgres.Changeset) {
	t.ChangesChan <- changeset
}

func (t *Truck) Stop() {
	select {
	case <-t.KillChan:
	default:
		close(t.KillChan)
	}
}
