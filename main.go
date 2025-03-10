package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/tonyfg/trucker/pkg/mainroutines"
	"github.com/tonyfg/trucker/pkg/truck"
)

func main() {
	// Command line flags for backfill operations
	backfillTruckName := flag.String("backfill-truck", "", "Name of the truck to backfill")
	backfillTables := flag.String("backfill-tables", "", "Comma-separated list of tables to backfill")
	flag.Parse()

	sigChan := trapSignals()
	projectPath := projectPathFromArgsOrCwd()
	doneChan, truckCfgs, trucksByInputConnection := mainroutines.Start(projectPath)

	// Handle backfill request if provided
	if *backfillTruckName != "" && *backfillTables != "" {
		tables := strings.Split(*backfillTables, ",")
		
		// Find the requested truck
		var targetTruck *truck.Truck
		for _, trucksList := range trucksByInputConnection {
			for _, t := range trucksList {
				if t.Name == *backfillTruckName {
					targetTruck = t
					break
				}
			}
			if targetTruck != nil {
				break
			}
		}
		
		if targetTruck != nil {
			log.Printf("Requesting backfill for truck %s, tables: %v\n", *backfillTruckName, tables)
			mainroutines.RequestBackfill(targetTruck, tables, trucksByInputConnection)
		} else {
			log.Printf("Error: Truck with name %s not found\n", *backfillTruckName)
		}
	}

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
