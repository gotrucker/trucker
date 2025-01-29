package main

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/jackc/pglogrepl"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/postgres"
	"github.com/tonyfg/trucker/pkg/truck"
)

// On startup:
// - Load main config and truck configs
// - Connect to databases
// - Run migrations
// - Check which trucks need backfills
//   - Run backfills
//   - Catch up replication on previously existing trucks up to the backfill snapshot LSN
//
// - Start trucks
func main() {
	sigChan := trapSignals()
	projectPath := projectPathFromArgsOrCwd()
	doneChan, truckCfgs, trucksByInputConnection := doTheThing(projectPath)

	if len(truckCfgs) > 0 {
	outerLoop:
		for {
			select {
			case <-sigChan:
				log.Println("Received termination signal. Stopping all trucks...")
				for _, trucks := range trucksByInputConnection {
					for _, truck := range trucks {
						truck.Stop()
					}
				}
				break outerLoop
			case exit := <-doneChan:
				log.Printf("Truck '%s' stopped early: %s\nBailing out...\n", exit.TruckName, exit.Msg)
				for _, trucks := range trucksByInputConnection {
					for _, truck := range trucks {
						truck.Stop()
					}
				}
				break outerLoop
			}
		}
	}

	log.Println("All trucks stopped. Exiting!")
}

func doTheThing(projectPath string) (chan truck.ExitMsg, []config.Truck, map[string][]*truck.Truck) {
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
			append(replicatedTablesPerConnection[connName], truckCfg.Input.Table)
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

	backfilledTables, backfillLSNs := backfill(replicationClients, trucksByInputConnection)
	catchup(replicationClients, trucksByInputConnection, backfilledTables, backfillLSNs)
	streamIt(trucksByInputConnection)

	return doneChan, truckCfgs, trucksByInputConnection
}

func backfill(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck) (map[string][]string, map[string]uint64) {
	backfillLSNs := make(map[string]uint64)
	backfilledTables := make(map[string][]string)

	for connName, rc := range replicationClients {
		tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
		defer rc.ResetStreamConn()
		log.Println("Backfill LSN", pglogrepl.LSN(backfillLSN))

		backfillLSNs[connName] = backfillLSN
		backfilledTables[connName] = tablesToBackfill

		for _, truck := range trucks[connName] {
			if slices.Contains(tablesToBackfill, truck.InputTable) {
				log.Printf("Backfilling truck '%s'...\n", truck.Name)
				truck.Backfill(snapshotName, backfillLSN)
			}
		}
	}

	return backfilledTables, backfillLSNs
}

func catchup(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck, skipTables map[string][]string, backfillLSNs map[string]uint64) {
	for connName, rc := range replicationClients {
		var startLSN uint64
		endLSN := backfillLSNs[connName]

		for _, truck := range trucks[connName] {
			if slices.Contains(skipTables[connName], truck.InputTable) {
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
				changesets := <-changesChan
				if changesets == nil {
					for _, truck := range trucks[connName] {
						truck.Writer.SetCurrentPosition(endLSN)
					}

					break
				}

				for changeset := range changesets {
					for _, truck := range trucks[connName] {
						if truck.InputTable == changeset.Table {
							truck.ProcessChangeset(changeset)
						}
					}
				}
			}
		}
	}
}

func streamIt(trucksByInputConnection map[string][]*truck.Truck) {
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
			changesets := <-changesChan

			for changeset := range changesets {
				for _, truck := range trucks {
					if truck.InputTable == changeset.Table {
						truck.ProcessChangeset(changeset)
					}
				}
			}
			// FIXME: advancing LSN on output DBs should happen right here instead of inside each truck
		}
	}
}

func trapSignals() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	return sigChan
}

func projectPathFromArgsOrCwd() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	return dir
}
