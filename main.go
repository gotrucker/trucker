package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tonyfg/trucker/pkg/mainroutines"
)

var version = "undefined"

func main() {
	log.Printf("Trucker version %s. Firing up the engine!\n", version)
	sigChan := trapSignals()
	projectPath := projectPathFromArgsOrCwd()
	doneChan, truckCfgs, trucksByInputConnection := mainroutines.Start(projectPath)

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
