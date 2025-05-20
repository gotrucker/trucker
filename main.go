package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/logging"
	"github.com/tonyfg/trucker/pkg/mainroutines"
)

var version = "undefined"
var log = logging.MakeSimpleLogger("main")

func main() {
	sigChan := trapSignals()

	projectPath := projectPathFromArgsOrCwd()
	ymlPath := filepath.Join(projectPath, "trucker.yml")
	cfg := config.Load(ymlPath)
	logging.Init(cfg)
	truckCfgs := config.LoadTrucks(projectPath, cfg)

	log.Info(fmt.Sprintf(
		"Trucker version %s. Configuration loaded... Firing up all %d trucks!\n",
		version,
		len(truckCfgs),
	))

	doneChan, truckCfgs, trucksByInputConnection := mainroutines.Start(cfg, truckCfgs)

	if len(truckCfgs) > 0 {
	outerLoop:
		for {
			select {
			case <-sigChan:
				log.Info("Received termination signal. Stopping all trucks...")
				for _, trucks := range trucksByInputConnection {
					for _, truck := range trucks {
						truck.Stop()
					}
				}
				break outerLoop
			case exit := <-doneChan:
				log.Info(fmt.Sprintf(
					"Truck '%s' stopped early: %s\nStopping all trucks and exiting...\n",
					exit.TruckName,
					exit.Msg,
				))
				for _, trucks := range trucksByInputConnection {
					for _, truck := range trucks {
						truck.Stop()
					}
				}
				break outerLoop
			}
		}
	}

	log.Info("All trucks stopped. Exiting!")
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
		log.Error(fmt.Sprintf("Can't get working directory: %s\nExiting...", err))
		os.Exit(1)
	}

	return dir
}
