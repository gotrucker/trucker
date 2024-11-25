package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jackc/pgx/v5"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/pg"
	"github.com/tonyfg/trucker/pkg/truck"
)

const projectPath = "test/fixtures/fake_project"

func main() {
	sigChan := trapSignals()
	ymlPath := filepath.Join(projectPath, "trucker.yml")
	cfg := config.Load(ymlPath)
	truckCfgs := config.LoadTrucks(projectPath, cfg)

	dbConnections := connectDatabases(cfg.Connections)
	defer disconnectDatabases(dbConnections)

	trucksByInputConnection := make(map[string][]truck.Truck)
	stoppedChan := make(chan truck.ExitMsg, len(truckCfgs))
	stopChans := make(map[string]chan any, 0)

	for _, truckCfg := range truckCfgs {
		stopChan := make(chan any)
		stopChans[truckCfg.Name] = stopChan
		truck := truck.NewTruck(truckCfg, dbConnections, stopChan, stoppedChan)
		trucksByInputConnection[truckCfg.Input.Connection] = append(trucksByInputConnection[truckCfg.Input.Connection], truck)
	}

	for _, trucks := range trucksByInputConnection {
		for _, truck := range trucks {
			fmt.Println(truck)
			// truck.Backfill()
			// cona cona cona
		}
	}

	if len(truckCfgs) > 0 {
	outerLoop:
		for {
			select {
			case <-sigChan:
				log.Println("Received termination signal. Stopping all trucks...")
				for _, killChan := range stopChans {
					killChan <- true
				}
				break outerLoop
			case exit := <-stoppedChan:
				log.Printf("Truck '%s' stopped early: %s\nBailing out...\n", exit.TruckName, exit.Msg)
				for name, killChan := range stopChans {
					if name == exit.TruckName {
						continue
					}
					killChan <- nil
				}
				break outerLoop
			}
		}
	}

	log.Println("All trucks stopped. Exiting!")
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
