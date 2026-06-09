package testharness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tonyfg/trucker/pkg/config"
	"github.com/tonyfg/trucker/pkg/postgres"
	"github.com/tonyfg/trucker/pkg/truck"
)

type selectedTest struct {
	truck config.Truck
	name  string
	path  string
}

func Run(projectPath, truckFilter, testFilter string) ([]Result, error) {
	cfg := config.Load(filepath.Join(projectPath, "trucker.yml"))
	trucks := config.LoadTrucks(projectPath, cfg)
	selected, err := selectTests(projectPath, trucks, truckFilter, testFilter)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no tests found")
	}

	results := make([]Result, 0, len(selected))
	for _, test := range selected {
		results = append(results, runOne(projectPath, cfg, test))
	}
	return results, nil
}

func selectTests(projectPath string, trucks []config.Truck, truckFilter, testFilter string) ([]selectedTest, error) {
	selected := make([]selectedTest, 0)
	foundTruck := truckFilter == ""
	for _, truckCfg := range trucks {
		if truckFilter != "" && truckCfg.Name != truckFilter {
			continue
		}
		foundTruck = true

		testsDir := filepath.Join(projectPath, truckCfg.Name, "tests")
		entries, err := os.ReadDir(testsDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			if testFilter != "" && entry.Name() != testFilter {
				continue
			}
			selected = append(selected, selectedTest{truck: truckCfg, name: entry.Name(), path: filepath.Join(testsDir, entry.Name())})
		}
	}
	if !foundTruck {
		return nil, fmt.Errorf("truck %q not found", truckFilter)
	}
	if testFilter != "" && len(selected) == 0 {
		return nil, fmt.Errorf("test %q not found", testFilter)
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].truck.Name == selected[j].truck.Name {
			return selected[i].name < selected[j].name
		}
		return selected[i].truck.Name < selected[j].truck.Name
	})
	return selected, nil
}

func runOne(projectPath string, cfg config.Config, test selectedTest) Result {
	start := time.Now()
	result := Result{TruckName: test.truck.Name, TestName: test.name, Status: "pass"}
	finishFail := func(phase string, err error) Result {
		result.Status = "fail"
		result.Phase = phase
		if err != nil {
			result.Error = err.Error()
		}
		result.Duration = time.Since(start)
		return result
	}

	inputConn := deriveTestConnection(cfg.Connections[test.truck.Input.Connection])
	outputConn := deriveTestConnection(cfg.Connections[test.truck.Output.Connection])
	testConns := cloneConnections(cfg.Connections)
	testConns[test.truck.Input.Connection] = inputConn
	testConns[test.truck.Output.Connection] = outputConn

	if err := ensureTestDB(context.Background(), inputConn); err != nil {
		return finishFail("ensure_input_db", err)
	}
	if inputConn.Name != outputConn.Name || inputConn.Adapter != outputConn.Adapter || inputConn.Database != outputConn.Database || inputConn.Host != outputConn.Host || inputConn.Port != outputConn.Port {
		if err := ensureTestDB(context.Background(), outputConn); err != nil {
			return finishFail("ensure_output_db", err)
		}
	}

	lsnTable := lsnTableName(test.truck.Input.Connection, cfg.UniqueId)
	slotPrefix := fmt.Sprintf("trucker_%s%s", inputConn.Database, cfg.UniqueId)
	if err := cleanupInputDB(context.Background(), inputConn, test.truck.Input.Tables, slotPrefix); err != nil {
		return finishFail("cleanup_input", err)
	}
	if err := cleanupOutputDB(context.Background(), outputConn, lsnTable); err != nil {
		return finishFail("cleanup_output", err)
	}

	if err := execScriptFile(context.Background(), inputConn, filepath.Join(test.path, "input_db_seed.sql")); err != nil {
		return finishFail("input_seed", err)
	}
	if err := execScriptFile(context.Background(), outputConn, filepath.Join(test.path, "output_db_seed.sql")); err != nil {
		return finishFail("output_seed", err)
	}

	timedCtx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	deadline := time.Now().Add(defaultTimeout)
	doneChan := make(chan truck.ExitMsg, 1)
	rc := postgres.NewReplicationClient(test.truck.Input.Tables, inputConn, cfg.UniqueId)
	var runningTruck truck.Truck
	truckStarted := false
	defer func() {
		if truckStarted {
			runningTruck.Stop()
		}
		rc.Close()
		if truckStarted {
			select {
			case <-doneChan:
			case <-time.After(2 * time.Second):
			}
		}
	}()

	tablesToBackfill, backfillLSN, snapshotName, err := setupReplicationWithPanicCapture(rc)
	if err != nil {
		return finishFail("backfill", err)
	}

	runningTruck = truck.NewTruck(test.truck, rc, testConns, doneChan, cfg.UniqueId)
	if err := callWithPanicCapture(func() { runningTruck.Backfill(snapshotName, backfillLSN, tablesToBackfill) }); err != nil {
		return finishFail("backfill", err)
	}
	rc.ResetStreamConn()
	rc.Register(postgres.Subscriber{
		Name:       runningTruck.Name,
		Tables:     tablesAsSet(runningTruck.InputTables),
		Ch:         runningTruck.TransactionChan,
		LsnFlushCh: runningTruck.LsnFlushCh,
		StartLSN:   runningTruck.Writer.GetCurrentPosition(),
		Done:       runningTruck.KillChan,
	})
	runningTruck.Start()
	truckStarted = true

	if err := execScriptFile(timedCtx, inputConn, filepath.Join(test.path, "stream_statements.sql")); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return finishFail("timeout", err)
		}
		return finishFail("stream_statements", err)
	}

	latestLSN, err := currentInputLSN(timedCtx, inputConn)
	if err != nil {
		return finishFail("latest_lsn", err)
	}

	rc.Start(runningTruck.Writer.GetCurrentPosition(), latestLSN)
	select {
	case <-rc.WaitDone():
	case <-timedCtx.Done():
		return finishFail("timeout", timedCtx.Err())
	}

	observed, err := waitForOutputLSN(timedCtx, outputConn, lsnTable, latestLSN, deadline)
	result.ObservedOutputLSN = observed
	result.TargetOutputLSN = latestLSN
	if err != nil {
		return finishFail("timeout", err)
	}

	expectationsSQL, err := os.ReadFile(filepath.Join(test.path, "expectations.sql"))
	if err != nil {
		return finishFail("expectations", err)
	}
	failures := runExpectations(timedCtx, outputConn, string(expectationsSQL))
	if len(failures) > 0 {
		result.Failures = failures
		return finishFail("expectations", nil)
	}

	result.Duration = time.Since(start)
	return result
}

func execScriptFile(ctx context.Context, conn config.Connection, path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return execScript(ctx, conn, string(contents))
}

func setupReplicationWithPanicCapture(rc *postgres.ReplicationClient) (tables []string, lsn uint64, snapshot string, err error) {
	err = callWithPanicCapture(func() {
		tables, lsn, snapshot = rc.Setup()
	})
	return tables, lsn, snapshot, err
}

func callWithPanicCapture(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	fn()
	return nil
}

func cloneConnections(conns map[string]config.Connection) map[string]config.Connection {
	cloned := make(map[string]config.Connection, len(conns))
	for name, conn := range conns {
		cloned[name] = conn
	}
	return cloned
}

func lsnTableName(inputConnectionName, uniqueID string) string {
	return fmt.Sprintf("trucker_current_lsn__%s%s", inputConnectionName, uniqueID)
}

func tablesAsSet(tables []string) map[string]bool {
	set := make(map[string]bool, len(tables))
	for _, table := range tables {
		set[table] = true
	}
	return set
}
