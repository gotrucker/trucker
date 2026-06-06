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
	Name                 string
	ReplicationClient    *postgres.ReplicationClient
	readQuery            string
	Reader               db.Reader
	InputTables          []string
	Writer               db.Writer
	OutputSql            string
	SlowQueryThresholdMs int64
	TransactionChan      chan db.Transaction
	LsnFlushCh           chan uint64 // receives AutoAdvance LSNs; debounced before flushing to output
	KillChan             chan any
	DoneChan             chan ExitMsg
}

func NewTruck(cfg config.Truck, rc *postgres.ReplicationClient, connCfgs map[string]config.Connection, doneChan chan ExitMsg, uniqueId string) Truck {
	return Truck{
		Name:                 cfg.Name,
		ReplicationClient:    rc,
		readQuery:            cfg.Input.Sql,
		Reader:               NewReader(cfg.Input.Sql, connCfgs[cfg.Input.Connection]),
		InputTables:          cfg.Input.Tables,
		Writer:               NewWriter(cfg.Input.Connection, cfg.Output.Sql, connCfgs[cfg.Output.Connection], uniqueId),
		SlowQueryThresholdMs: cfg.SlowQueryThresholdMs,
		TransactionChan:      make(chan db.Transaction),
		LsnFlushCh:           make(chan uint64, 1),
		KillChan:             make(chan any),
		DoneChan:             doneChan,
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
		log.Printf("[Truck %s] Setting up stream position tracking in output database...\n", t.Name)
		t.Writer.SetupPositionTracking()
		t.Writer.SetCurrentPosition(targetLSN)
	}
	log.Printf("[Truck %s] Backfill complete in %f seconds!\n", t.Name, time.Since(start).Seconds())
}

const lsnFlushDelay = 3 * time.Second

func (t *Truck) Start() {
	log.Printf("[Truck %s] Starting to read from replication stream...\n", t.Name)

	go func() {
		defer func() {
			t.DoneChan <- ExitMsg{t.Name, "Exited!"}
		}()

		var pendingFlushLSN uint64
		flushTimer := time.NewTimer(0)
		flushTimer.Stop()

		for {
			select {
			case transaction, ok := <-t.TransactionChan:
				if !ok {
					log.Printf("[Truck %s] Transaction channel closed. Exiting...\n", t.Name)
					return
				}

				// Cancel any pending deferred flush — a real transaction supersedes it.
				if !flushTimer.Stop() {
					select {
					case <-flushTimer.C:
					default:
					}
				}
				pendingFlushLSN = 0

				// TODO: All of the following should be written in a single transaction, including the SetCurrentPosition call
				now := time.Now()
				for changes := range transaction.Changes {
					resultChangeset := t.Reader.Read(changes)
					if time.Since(now).Milliseconds() > t.SlowQueryThresholdMs {
						log.Printf("[Truck %s] Slow input query: took %dms for %d columns.\n", t.Name, time.Since(now).Milliseconds(), len(changes.Columns))
					}

					now = time.Now()
					t.Writer.Write(resultChangeset)
					if time.Since(now).Milliseconds() > t.SlowQueryThresholdMs {
						log.Printf("[Truck %s] Slow output query: took %dms for %d columns.\n", t.Name, time.Since(now).Milliseconds(), len(resultChangeset.Columns))
					}
				}

				if transaction.StreamPosition != 0 {
					t.Writer.SetCurrentPosition(transaction.StreamPosition)
					t.ReplicationClient.AckLSN(t.Name, transaction.StreamPosition)
				}

			case lsn := <-t.LsnFlushCh:
				if lsn > pendingFlushLSN {
					pendingFlushLSN = lsn
				}
				flushTimer.Reset(lsnFlushDelay)

			case <-flushTimer.C:
				if pendingFlushLSN != 0 {
					t.Writer.SetCurrentPosition(pendingFlushLSN)
					t.ReplicationClient.AckLSN(t.Name, pendingFlushLSN)
					pendingFlushLSN = 0
				}

			case <-t.KillChan:
				log.Printf("[Truck %s] Received kill signal. Exiting...\n", t.Name)
				t.Reader.Close()
				t.Writer.Close()
				return
			}
		}
	}()
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

func NewWriter(inputConnectionName string, outputSql string, cfg config.Connection, uniqueId string) db.Writer {
	switch cfg.Adapter {
	case "postgres":
		return postgres.NewWriter(inputConnectionName, outputSql, cfg, uniqueId)
	case "clickhouse":
		return clickhouse.NewWriter(inputConnectionName, outputSql, cfg, uniqueId)
	default:
		log.Fatalf("Unsupported adapter: %s", cfg.Adapter)
	}

	return nil
}
