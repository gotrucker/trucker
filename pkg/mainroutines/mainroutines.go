package mainroutines

import (
	"log"
	"path/filepath"

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
			replicationClients[connName] = postgres.NewReplicationClient(replicatedTables, cfg.Connections[connName], cfg.UniqueId)
		}
	}

	trucksByInputConnection := make(map[string][]*truck.Truck)
	for _, truckCfg := range truckCfgs {
		truck := truck.NewTruck(truckCfg, replicationClients[truckCfg.Input.Connection], cfg.Connections, doneChan, cfg.UniqueId)
		trucksByInputConnection[truckCfg.Input.Connection] = append(trucksByInputConnection[truckCfg.Input.Connection], &truck)
	}

	go func() {
		backfillLSNs := backfill(replicationClients, trucksByInputConnection)
		catchup(replicationClients, trucksByInputConnection, backfillLSNs)
		streamChanges(trucksByInputConnection)
	}()

	return doneChan, truckCfgs, trucksByInputConnection
}

func backfill(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck) map[string]uint64 {
	backfillLSNs := make(map[string]uint64)

	for connName, rc := range replicationClients {
		tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
		defer rc.ResetStreamConn()
		log.Println("Backfill LSN", pglogrepl.LSN(backfillLSN))

		for _, truck := range trucks[connName] {
			truck.Backfill(snapshotName, backfillLSN, tablesToBackfill)
		}

		backfillLSNs[connName] = backfillLSN
	}

	return backfillLSNs
}

func catchup(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck, backfillLSNs map[string]uint64) {
	for connName, rc := range replicationClients {
		var startLSN uint64
		endLSN := backfillLSNs[connName]

		for _, truck := range trucks[connName] {
			log.Printf("[Truck %s] Catching up to latest stream position...\n", truck.Name)

			truckLSN := truck.Writer.GetCurrentPosition()
			if truckLSN < startLSN || startLSN == 0 {
				startLSN = truckLSN
			}

			truck.Start()
		}

		if startLSN > 0 && endLSN > 0 {
			changesChan := rc.Start(startLSN, endLSN)
			for transaction := range changesChan {
				for _, truck := range trucks[connName] {
					truck.ProcessChangeset(transaction)
				}

				if transaction.StreamPosition > 0 {
					rc.SetProcessedLSN(transaction.StreamPosition)
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
		for transaction := range changesChan {
			for _, truck := range trucks {
				truck.ProcessChangeset(transaction)
			}

			if transaction.StreamPosition > 0 {
				rc.SetProcessedLSN(transaction.StreamPosition)
			}
		}
	}
}
