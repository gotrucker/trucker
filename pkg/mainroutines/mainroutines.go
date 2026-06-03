package mainroutines

import (
	"log"
	"path/filepath"

	"github.com/jackc/pglogrepl"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/postgres"
	"github.com/tonyfg/trucker/pkg/truck"
)

func Start(projectPath string) (chan truck.ExitMsg, []config.Truck, map[string][]*truck.Truck, map[string]*postgres.ReplicationClient) {
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
		t := truck.NewTruck(truckCfg, replicationClients[truckCfg.Input.Connection], cfg.Connections, doneChan, cfg.UniqueId)
		trucksByInputConnection[truckCfg.Input.Connection] = append(trucksByInputConnection[truckCfg.Input.Connection], &t)
	}

	go func() {
		backfillLSNs := backfill(replicationClients, trucksByInputConnection)

		// Register subscribers after backfill so each truck's LSN is accurate.
		for connName, rc := range replicationClients {
			for _, t := range trucksByInputConnection[connName] {
				rc.Register(postgres.Subscriber{
					Name:     t.Name,
					Tables:   tablesAsSet(t.InputTables),
					Ch:       t.TransactionChan,
					StartLSN: t.Writer.GetCurrentPosition(),
					Done:     t.KillChan,
				})
			}
		}

		// Start all truck goroutines once.
		for _, trucks := range trucksByInputConnection {
			for _, t := range trucks {
				t.Start()
			}
		}

		catchup(replicationClients, trucksByInputConnection, backfillLSNs)
		streamChanges(replicationClients, trucksByInputConnection)
	}()

	return doneChan, truckCfgs, trucksByInputConnection, replicationClients
}

func tablesAsSet(tables []string) map[string]bool {
	set := make(map[string]bool, len(tables))
	for _, t := range tables {
		set[t] = true
	}
	return set
}

func backfill(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck) map[string]uint64 {
	backfillLSNs := make(map[string]uint64)

	for connName, rc := range replicationClients {
		tablesToBackfill, backfillLSN, snapshotName := rc.Setup()
		defer rc.ResetStreamConn()
		log.Println("Backfill LSN", pglogrepl.LSN(backfillLSN))

		for _, t := range trucks[connName] {
			t.Backfill(snapshotName, backfillLSN, tablesToBackfill)
		}

		backfillLSNs[connName] = backfillLSN
	}

	return backfillLSNs
}

func catchup(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck, backfillLSNs map[string]uint64) {
	for connName, rc := range replicationClients {
		endLSN := backfillLSNs[connName]
		startLSN := rc.MinTruckLSN()

		log.Printf("[catchup] conn=%s startLSN=%d endLSN=%d\n", connName, startLSN, endLSN)

		if startLSN > 0 && endLSN > 0 {
			rc.Start(startLSN, endLSN)
			<-rc.WaitDone()
		}

		for _, t := range trucks[connName] {
			log.Printf("[Truck %s] Caught up to stream position %d\n", t.Name, t.Writer.GetCurrentPosition())
		}
	}
}

func streamChanges(replicationClients map[string]*postgres.ReplicationClient, trucks map[string][]*truck.Truck) {
	for connName, rc := range replicationClients {
		// Stream starts from the lowest truck LSN so no truck loses data on restart.
		startLSN := rc.MinTruckLSN()
		log.Printf("[stream] conn=%s startLSN=%d\n", connName, startLSN)

		// Close RC when all trucks on this connection are stopped.
		go func(rc *postgres.ReplicationClient, truckList []*truck.Truck) {
			for _, t := range truckList {
				<-t.KillChan
			}
			rc.Close()
		}(rc, trucks[connName])

		rc.Start(startLSN, 0)
	}
}
