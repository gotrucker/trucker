package truck

import (
	"log"
	"slices"
	"time"

	"github.com/tonyfg/trucker/pkg/clickhouse"
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
	readQuery         string
	Reader            db.Reader
	InputTables       []string
	Writer            db.Writer
	OutputTable       string
	OutputSql         string
	ChangesChan       chan *db.Changeset
	KillChan          chan any
	DoneChan          chan ExitMsg
	CurrentPosition   uint64
}

func NewTruck(cfg config.Truck, rc *postgres.ReplicationClient, connCfgs map[string]config.Connection, doneChan chan ExitMsg) Truck {
	return Truck{
		Name:              cfg.Name,
		ReplicationClient: rc,
		readQuery:         cfg.Input.Sql,
		Reader:            NewReader(cfg.Input.Sql, connCfgs[cfg.Input.Connection]),
		InputTables:       cfg.Input.Tables,
		Writer:            NewWriter(cfg.Input.Connection, cfg.Output.Sql, connCfgs[cfg.Output.Connection]),
		ChangesChan:       make(chan *db.Changeset),
		KillChan:          make(chan any),
		DoneChan:          doneChan,
	}
}

func (t *Truck) Backfill(snapshotName string, targetLSN uint64, allTables []string) {
	tables := make([]string, 0)
	for _, table := range allTables {
		if slices.Contains(t.InputTables, table) {
			tables = append(tables, table)
		}
	}

	if len(tables) == 0 {
		return
	}

	start := time.Now()
	log.Printf("[Truck %s] Running backfill for tables: %v\n", t.Name, tables)

	for _, table := range tables {
		changeset := t.ReplicationClient.ReadBackfillData(table, snapshotName, t.readQuery)
		t.Writer.Write(changeset)
	}

	curPos := t.Writer.GetCurrentPosition()
	if curPos == 0 {
		t.Writer.SetupPositionTracking()
		t.Writer.SetCurrentPosition(targetLSN)
		t.CurrentPosition = targetLSN
	} else {
		t.CurrentPosition = curPos
	}

	log.Printf("[Truck %s] Backfill complete in %f seconds!\n", t.Name, time.Since(start).Seconds())
}

func (t *Truck) Start() {
	log.Printf("[Truck %s] Starting to read from replication stream...\n", t.Name)

	go func() {
		defer func() {
			t.DoneChan <- ExitMsg{t.Name, "Exited!"}
		}()

		for {
			select {
			case changeset := <-t.ChangesChan:
				if changeset == nil {
					log.Printf("[Truck %s] Changeset channel closed. Exiting...\n", t.Name)
					return
				}

				resultChangeset := t.Reader.Read(changeset)
				if resultChangeset == nil {
					continue
				}

				t.Writer.WithTransaction(func() {
					t.Writer.Write(resultChangeset)
				})
			case <-t.KillChan:
				log.Printf("[Truck %s] Received kill msg. Exiting...\n", t.Name)
				t.ReplicationClient.Close()
				t.Reader.Close()
				t.Writer.Close()
				close(t.ChangesChan)
				return
			}
		}
	}()
}

func (t *Truck) ProcessChangeset(changeset *db.Changeset) {
	t.ChangesChan <- changeset
}

func (t *Truck) Stop() {
	select {
	case <-t.KillChan:
	default:
		close(t.KillChan)
	}
}

func NewReader(inputSql string, cfg config.Connection) db.Reader {
	switch cfg.Adapter {
	case "postgres":
		return postgres.NewReader(inputSql, cfg)
	case "clickhouse":
		log.Fatalf("Clickhouse is not supported as an input source")
	default:
		log.Fatalf("Unsupported adapter: %s", cfg.Adapter)
	}

	return nil
}

func NewWriter(inputConnectionName string, outputSql string, cfg config.Connection) db.Writer {
	switch cfg.Adapter {
	case "postgres":
		return postgres.NewWriter(inputConnectionName, outputSql, cfg)
	case "clickhouse":
		return clickhouse.NewWriter(inputConnectionName, outputSql, cfg)
	default:
		log.Fatalf("Unsupported adapter: %s", cfg.Adapter)
	}

	return nil
}
