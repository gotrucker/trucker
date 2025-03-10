package mainroutines

import (
	"log"
	"path/filepath"
	"slices"

	"github.com/jackc/pglogrepl"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/postgres"
	"github.com/tonyfg/trucker/pkg/truck"
)

func Start(projectPath string) (chan truck.ExitMsg, []config.Truck, map[string][]*truck.Truck) {
	ymlPath := filepath.Join(projectPath, "trucker.yml")
	cfg := config.Load(ymlPath)
	truckCfgs := config.LoadTrucks(projectPath, cfg)
	doneChan := make(chan truck.ExitMsg, len(truckCfgs))

	replicatedTablesPerConnection := make(map[string][]string)
	for _, truckCfg := range truckCfgs {
		connName := truckCfg.Input.Connection
		if _, ok := replicatedTablesPerConnection[connName]; !ok {
			replicatedTablesPerConnection[connName] = make([]string, 0, 1)
		}
		replicatedTablesPerConnection[connName] =
			append(replicatedTablesPerConnection[connName], truckCfg.Input.Tables...)
	}

	replicationClients := make(map[string]*postgres.ReplicationClient)
	for _, truckCfg := range truckCfgs {
		connName := truckCfg.Input.Connection
		if _, ok := replicationClients[connName]; !ok {
			replicatedTables := replicatedTablesPerConnection[connName]
			replicationClients[connName] = postgres.NewReplicationClient(replicatedTables, cfg.Connections[connName])
		}
	}

	trucksByInputConnection := make(map[string][]*truck.Truck)
	for _, truckCfg := range truckCfgs {
		truck := truck.NewTruck(truckCfg, replicationClients[truckCfg.Input.Connection], cfg.Connections, doneChan)
		trucksByInputConnection[truckCfg.Input.Connection] = append(trucksByInputConnection[truckCfg.Input.Connection], &truck)
	}

	go func() {
		backfilledTables, backfillLSNs := backfill(replicationClients, trucksByInputConnection)
		catchup(replicationClients, trucksByInputConnection, backfilledTables, backfillLSNs)
		streamChanges(trucksByInputConnection)
	}()

	return doneChan, truckCfgs, trucksByInputConnection
}

func backfill(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck) (map[string][]string, map[string]uint64) {
	backfillLSNs := make(map[string]uint64)
	backfilledTables := make(map[string][]string)

	for connName, rc := range replicationClients {
		tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
		defer rc.ResetStreamConn()
		log.Println("Backfill LSN", pglogrepl.LSN(backfillLSN))

		for _, truck := range trucks[connName] {
			truck.Backfill(snapshotName, backfillLSN, tablesToBackfill)
		}

		backfillLSNs[connName] = backfillLSN
		backfilledTables[connName] = tablesToBackfill
	}

	return backfilledTables, backfillLSNs
}

// RequestBackfill initiates a backfill for specific tables on a specific truck
// without pausing streaming for other trucks
func RequestBackfill(targetTruck *truck.Truck, tables []string, trucksByInputConnection map[string][]*truck.Truck) {
	if len(tables) == 0 {
		return
	}
	
	// Filter tables to only those that are configured for this truck
	tablesToBackfill := make([]string, 0)
	for _, table := range tables {
		if slices.Contains(targetTruck.InputTables, table) {
			tablesToBackfill = append(tablesToBackfill, table)
		}
	}
	
	if len(tablesToBackfill) == 0 {
		log.Printf("[Truck %s] No matching tables to backfill\n", targetTruck.Name)
		return
	}
	
	// Store connection name for restarting replication later
	connName := ""
	for conn, trucks := range trucksByInputConnection {
		for _, t := range trucks {
			if t == targetTruck {
				connName = conn
				break
			}
		}
		if connName != "" {
			break
		}
	}
	
	// Start backfill in a separate goroutine to not block
	go func() {
		targetTruck.BackfillIndependently(tablesToBackfill)
		
		// After backfill completes, restart replication streams for all trucks on this connection
		// to ensure backfilled truck can catch up
		if connName != "" {
			RestartReplicationStreams(connName, trucksByInputConnection)
		}
	}()
}

// RestartReplicationStreams restarts all replication streams for a given connection
// to ensure backfilled trucks can catch up with ongoing streaming
func RestartReplicationStreams(connName string, trucksByInputConnection map[string][]*truck.Truck) {
	trucks := trucksByInputConnection[connName]
	if len(trucks) == 0 {
		return
	}
	
	// Get the replication client (all trucks for this connection share the same client)
	rc := trucks[0].ReplicationClient
	
	// Stop all current streams for these trucks
	for _, t := range trucks {
		// Don't close the channels, just pause processing
		t.PauseProcessing()
	}
	
	// Find the oldest LSN among all trucks to ensure all catch up
	var oldestLSN uint64
	for _, t := range trucks {
		truckLSN := t.Writer.GetCurrentPosition()
		if truckLSN < oldestLSN || oldestLSN == 0 {
			oldestLSN = truckLSN
		}
	}
	
	log.Printf("Restarting replication streams for connection %s from LSN %d", connName, oldestLSN)
	
	// Reset the stream connection
	rc.ResetStreamConn()
	
	// Resume all trucks
	for _, t := range trucks {
		t.ResumeProcessing()
	}
	
	// Restart replication stream from the oldest LSN
	changesChan := rc.Start(oldestLSN, 0)
	
	// Process changes in a new goroutine
	go func() {
		for {
			transaction := <-changesChan
			
			if transaction == nil {
				break
			}
			
			for changeset := range transaction.Changesets {
				for _, t := range trucks {
					if slices.Contains(t.InputTables, changeset.Table) {
						t.ProcessChangeset(changeset)
					}
				}
			}
			
			if transaction.Position > 0 {
				rc.SetWrittenLSN(transaction.Position)
			}
		}
	}()
}

func catchup(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck, skipTables map[string][]string, backfillLSNs map[string]uint64) {
	for connName, rc := range replicationClients {
		var startLSN uint64
		endLSN := backfillLSNs[connName]

		for _, truck := range trucks[connName] {
			needsCatchup := false
			for _, table := range truck.InputTables {
				if !slices.Contains(skipTables[connName], table) {
					needsCatchup = true
				}
			}

			if !needsCatchup {
				continue
			}

			log.Printf("[Truck %s] Catching up to latest stream position...\n", truck.Name)

			truckLSN := truck.Writer.GetCurrentPosition()
			if truckLSN < startLSN || startLSN == 0 {
				startLSN = truckLSN
			}

			truck.Start()
		}

		if startLSN > 0 && endLSN > 0 {
			changesChan := rc.Start(startLSN, endLSN)
			for {
				transaction := <-changesChan
				if transaction == nil {
					for _, truck := range trucks[connName] {
						truck.Writer.SetCurrentPosition(endLSN)
					}

					break
				}

				for changeset := range transaction.Changesets {
					for _, truck := range trucks[connName] {
						if !slices.Contains(skipTables[connName], changeset.Table) &&
							slices.Contains(truck.InputTables, changeset.Table) {
							truck.ProcessChangeset(changeset)
						}
					}
				}

				if transaction.Position > 0 {
					rc.SetWrittenLSN(transaction.Position)
				}
			}
		}
	}
}

func streamChanges(trucksByInputConnection map[string][]*truck.Truck) {
	trucksByRc := make(map[*postgres.ReplicationClient][]*truck.Truck)
	for _, trucks := range trucksByInputConnection {
		for _, t := range trucks {
			if _, ok := trucksByRc[t.ReplicationClient]; !ok {
				trucksByRc[t.ReplicationClient] = make([]*truck.Truck, 0, 1)
			}

			trucksByRc[t.ReplicationClient] = append(trucksByRc[t.ReplicationClient], t)
		}
	}

	for rc, trucks := range trucksByRc {
		var startLSN uint64

		for _, t := range trucks {
			truckLSN := t.Writer.GetCurrentPosition()

			if truckLSN < startLSN || startLSN == 0 {
				startLSN = truckLSN
			}

			t.Start()
		}

		changesChan := rc.Start(startLSN, 0)
		for {
			transaction := <-changesChan

			if transaction != nil {
				for changeset := range transaction.Changesets {
					for _, truck := range trucks {
						if slices.Contains(truck.InputTables, changeset.Table) {
							truck.ProcessChangeset(changeset)
						}
					}
				}

				if transaction.Position > 0 {
					rc.SetWrittenLSN(transaction.Position)
				}
			}
		}
	}
}
