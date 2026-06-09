package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tonyfg/trucker/pkg/mainroutines"
	"github.com/tonyfg/trucker/pkg/testharness"
)

var version = "undefined"

func main() {
	log.Printf("Trucker version %s. Firing up the engine!\n", version)
	if len(os.Args) > 1 && (os.Args[1] == "-gen" || os.Args[1] == "-test") {
		os.Exit(testharness.Dispatch(os.Args[1:], mustCwd(), os.Stdout, os.Stderr))
	}

	sigChan := trapSignals()
	projectPath := projectPathFromArgsOrCwd()
	doneChan, truckCfgs, trucksByInputConnection, _, metricsSrv := mainroutines.Start(projectPath, version)

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

	if metricsSrv != nil {
		metricsSrv.Shutdown(context.Background())
	}
	log.Println("All trucks stopped. Exiting!")
}

func trapSignals() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
	return sigChan
}

func mustCwd() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return dir
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
