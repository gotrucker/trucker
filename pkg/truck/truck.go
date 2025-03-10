package truck

import (
	"log"
	"slices"
	"sync"
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
	pauseProcessing   bool
	processingLock    sync.Mutex
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

// BackfillIndependently runs a backfill for a single truck without interrupting streaming
// on other trucks by using a dedicated replication client
func (t *Truck) BackfillIndependently(tables []string) {
	if len(tables) == 0 {
		return
	}
	
	start := time.Now()
	log.Printf("[Truck %s] Running independent backfill for tables: %v\n", t.Name, tables)
	
	// Get a standalone backfill client
	backfillRC, snapshotName, targetLSN := t.ReplicationClient.CreateBackfillClient()
	defer backfillRC.Close() // Clean up when done
	
	// Run backfill with the separate client
	for _, table := range tables {
		changeset := backfillRC.ReadBackfillData(table, snapshotName, t.readQuery)
		t.Writer.Write(changeset)
	}
	
	// Update position tracking
	curPos := t.Writer.GetCurrentPosition()
	if curPos == 0 {
		t.Writer.SetupPositionTracking()
		t.Writer.SetCurrentPosition(targetLSN)
		t.CurrentPosition = targetLSN
	} else {
		// Only update if the backfill LSN is newer
		if targetLSN > curPos {
			t.Writer.SetCurrentPosition(targetLSN)
			t.CurrentPosition = targetLSN
		} else {
			t.CurrentPosition = curPos
		}
	}
	
	log.Printf("[Truck %s] Independent backfill complete in %f seconds!\n", t.Name, time.Since(start).Seconds())
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
	t.processingLock.Lock()
	isPaused := t.pauseProcessing
	t.processingLock.Unlock()
	
	if !isPaused {
		t.ChangesChan <- changeset
	}
}

// PauseProcessing pauses the truck's processing of changesets temporarily
// useful when restarting replication streams
func (t *Truck) PauseProcessing() {
	t.processingLock.Lock()
	defer t.processingLock.Unlock()
	t.pauseProcessing = true
	log.Printf("[Truck %s] Paused processing", t.Name)
}

// ResumeProcessing resumes the truck's processing of changesets
func (t *Truck) ResumeProcessing() {
	t.processingLock.Lock()
	defer t.processingLock.Unlock()
	t.pauseProcessing = false
	log.Printf("[Truck %s] Resumed processing", t.Name)
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
