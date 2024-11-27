package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pglogrepl"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/pg"
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

	dbConnections := connectDatabases(cfg.Connections)
	defer disconnectDatabases(dbConnections)

	replicatedTablesPerConnection := make(map[string][]string)
	for _, truckCfg := range truckCfgs {
		connName := truckCfg.Input.Connection
		if _, ok := replicatedTablesPerConnection[connName]; !ok {
			replicatedTablesPerConnection[connName] = make([]string, 0, 1)
		}
		replicatedTablesPerConnection[connName] =
			append(replicatedTablesPerConnection[connName], truckCfg.Input.Table)
	}

	replicationClients := make(map[string]*pg.ReplicationClient)
	for _, truckCfg := range truckCfgs {
		connName := truckCfg.Input.Connection
		if _, ok := replicationClients[connName]; !ok {
			replicatedTables := replicatedTablesPerConnection[connName]
			replicationClients[connName] = pg.NewReplicationClient(replicatedTables, cfg.Connections[connName])
		}
	}

	trucksByInputConnection := make(map[string][]*truck.Truck)
	for _, truckCfg := range truckCfgs {
		truck := truck.NewTruck(truckCfg, replicationClients[truckCfg.Input.Connection], dbConnections, doneChan)
		trucksByInputConnection[truckCfg.Input.Connection] = append(trucksByInputConnection[truckCfg.Input.Connection], &truck)
	}

	backfilledTables, backfillLSNs := backfill(replicationClients, trucksByInputConnection)
	catchup(replicationClients, trucksByInputConnection, backfilledTables, backfillLSNs)
	streamIt(trucksByInputConnection)

	return doneChan, truckCfgs, trucksByInputConnection
}

func backfill(replicationClients map[string]*pg.ReplicationClient, trucks map[string][]*truck.Truck) (map[string][]string, map[string]int64) {
	backfillLSNs := make(map[string]int64)
	backfilledTables := make(map[string][]string)

	for connName, rc := range replicationClients {
		tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
		defer rc.ResetStreamConn()
		fmt.Println("Backfill LSN", pglogrepl.LSN(backfillLSN))

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

func catchup(replicationClients map[string]*pg.ReplicationClient, trucks map[string][]*truck.Truck, skipTables map[string][]string, backfillLSNs map[string]int64) {
	for connName, rc := range replicationClients {
		var startLSN int64
		endLSN := backfillLSNs[connName]

		for _, truck := range trucks[connName] {
			if slices.Contains(skipTables[connName], truck.InputTable) {
				fmt.Printf("Skipping catchup for truck '%s' because we just finished backfilling it...\n", truck.Name)
				continue
			} else {
				fmt.Printf("FUUUUUUUUUUU catchup for truck '%s'...\n", truck.Name)
			}

			fmt.Println("Fodasse o uqe eustou a fazer aqui")

			truckLSN := truck.Writer.GetCurrentLsn()

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
						truck.Writer.SetCurrentLsn(endLSN)
					}

					break
				}

				for table, changeset := range changesets {
					for _, truck := range trucks[connName] {
						if truck.InputTable == table {
							truck.ProcessChangeset(changeset)
						}
					}
				}
			}
		}
	}
}

func streamIt(trucksByInputConnection map[string][]*truck.Truck) {
	fmt.Println("STREAAAAAAAAAAM ITTTTTTTTTTTTTT")
	trucksByRc := make(map[*pg.ReplicationClient][]*truck.Truck)
	for _, trucks := range trucksByInputConnection {
		for _, t := range trucks {
			if _, ok := trucksByRc[t.ReplicationClient]; !ok {
				trucksByRc[t.ReplicationClient] = make([]*truck.Truck, 0, 1)
			}

			trucksByRc[t.ReplicationClient] = append(trucksByRc[t.ReplicationClient], t)
		}
	}

	for rc, trucks := range trucksByRc {
		var startLSN int64

		for _, t := range trucks {
			truckLSN := t.Writer.GetCurrentLsn()

			if truckLSN < startLSN || startLSN == 0 {
				startLSN = truckLSN
			}

			t.Start()
		}

		changesChan := rc.Start(startLSN, 0)
		for {
			changesets := <-changesChan

			for table, changeset := range changesets {
				for _, truck := range trucks {
					if truck.InputTable == table {
						truck.ProcessChangeset(changeset)
					}
				}
			}
		}
	}
}

func trapSignals() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	return sigChan
}

func connectDatabases(connectionCfgs map[string]config.Connection) map[string]*pgx.Conn {
	connections := make(map[string]*pgx.Conn)

	for _, connectionCfg := range connectionCfgs {
		if connectionCfg.Adapter == "postgres" {
			connections[connectionCfg.Name] = pgConnect(connectionCfg)
		} else {
			log.Fatalf("Unsupported connection adapter: %s", connectionCfg.Adapter)
		}
	}

	return connections
}

func disconnectDatabases(connections map[string]*pgx.Conn) {
	for _, connection := range connections {
		connection.Close(context.Background())
	}
}

func pgConnect(connectionCfg config.Connection) *pgx.Conn {
	return pg.NewConnection(
		connectionCfg.User,
		connectionCfg.Pass,
		connectionCfg.Host,
		connectionCfg.Port,
		connectionCfg.Database,
		false,
	)
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
