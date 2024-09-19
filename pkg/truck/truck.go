package truck

import (
	"fmt"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/db"
)

type ExitMsg struct {
	TruckName string
	Msg       string
}

func Start(cfg config.Truck, dbConnections map[string]db.Db, killChan chan any, exitedChan chan ExitMsg) {
	fmt.Println("Starting truck", cfg.Name)
	defer func() {
		exitedChan <- ExitMsg{cfg.Name, "Nothing to see here"}
	}()
	fmt.Println("Stopping truck", cfg.Name)
}
