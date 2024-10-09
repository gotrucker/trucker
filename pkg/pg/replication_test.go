package pg

import (
	"context"
	"testing"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/test/helpers"
)

var connectionCfg = config.Connection{
	Name:     "test",
	Adapter:  "pg",
	Host:     "pg_input",
	Port:     5432,
	Database: "trucker",
	User:     "trucker",
}

func TestSetup(t *testing.T) {
	conn := helpers.Connect(connectionCfg)
	helpers.LoadTestDb(conn)

	replClient := NewReplicationClient([][]string{{"public", "countries"}}, connectionCfg)
	tablesToBackfill, _, snapshotName := replClient.Setup()

	// Jamaica should not show up in the backfill, since it was added after the snapshot
	_, err := conn.Exec(context.Background(), "INSERT INTO countries (name) VALUES ('Jamaica')")
	if err != nil {
		t.Error(err)
	}

	for _, table := range tablesToBackfill {
		replClient.Backfill(table, snapshotName)
	}
}
