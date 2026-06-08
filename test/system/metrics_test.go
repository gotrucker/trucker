package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tonyfg/trucker/pkg/mainroutines"
	"github.com/tonyfg/trucker/test/helpers"
)

const metricsAddr = ":9091"
const metricsURL = "http://localhost:9091/metrics"

func TestMetricsExport(t *testing.T) {
	conn := helpers.PreparePostgresTestDb()
	defer conn.Close(context.Background())

	// Clone the postgres_to_postgres fixture so we can inject metrics_addr without
	// disturbing the shared fixture directory used by other tests.
	tmpPath := Basepath + "/../../tmp/metrics_test"
	os.RemoveAll(tmpPath)
	if err := copyDir(Basepath+"/../fixtures/projects/postgres_to_postgres", tmpPath); err != nil {
		t.Fatal("copyDir:", err)
	}

	// Inject metrics_addr into the cloned trucker.yml.
	ymlPath := tmpPath + "/trucker.yml"
	f, err := os.OpenFile(ymlPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal("open trucker.yml:", err)
	}
	fmt.Fprintf(f, "\nmetrics_addr: \"%s\"\n", metricsAddr)
	f.Close()

	_, _, trucksByInputConnection, rcClients, metricsSrv := mainroutines.Start(tmpPath, "test")
	defer func() {
		for _, trucks := range trucksByInputConnection {
			for _, truck := range trucks {
				truck.Stop()
			}
		}
		for _, rc := range rcClients {
			rc.Close()
			<-rc.WaitDone()
		}
		if metricsSrv != nil {
			metricsSrv.Shutdown(context.Background())
		}
	}()

	if metricsSrv == nil {
		t.Fatal("expected metrics server to be started but it was nil")
	}

	// Wait for the metrics HTTP server to be ready.
	if err := waitFor(t, 5*time.Second, func() bool {
		resp, err := http.Get(metricsURL)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}); err != nil {
		t.Fatal("metrics server did not start:", err)
	}

	// trucker_build_info should be present immediately after startup.
	text := fetchMetrics(t)
	assertMetric(t, text, "trucker_build_info", map[string]string{"version": "test"}, 1, true)

	// Wait for backfill: the fixture DB has 4 whiskies seeded by PreparePostgresTestDb.
	if err := waitFor(t, 10*time.Second, func() bool {
		var cnt int
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat").Scan(&cnt)
		return cnt >= 4
	}); err != nil {
		t.Fatal("backfill did not complete:", err)
	}

	// Insert a new row and wait for it to be streamed.
	conn.Exec(context.Background(), "INSERT INTO public.whiskies (name, age, whisky_type_id) VALUES ('Test Whisky', 5, 1)")
	if err := waitFor(t, 10*time.Second, func() bool {
		var cnt int
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat WHERE name = 'Test Whisky'").Scan(&cnt)
		return cnt == 1
	}); err != nil {
		t.Fatal("insert was not streamed:", err)
	}

	// Update it (change name so we can detect the change unambiguously) and wait.
	conn.Exec(context.Background(), "UPDATE public.whiskies SET name = 'Test Whisky Updated', age = 10 WHERE name = 'Test Whisky'")
	if err := waitFor(t, 10*time.Second, func() bool {
		var cnt int
		conn.QueryRow(context.Background(), "SELECT count(*) FROM whiskies_flat WHERE name = 'Test Whisky Updated'").Scan(&cnt)
		return cnt == 1
	}); err != nil {
		t.Fatal("update was not streamed:", err)
	}

	text = fetchMetrics(t)

	// Counters that must be > 0 after processing inserts + updates.
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{
			"trucker_transactions_total",
			map[string]string{"truck": "truck", "input_db": "pg_input_conn", "output_db": "pg_input_conn"},
		},
		{
			"trucker_rows_written_total",
			map[string]string{"truck": "truck", "table": "public.whiskies", "operation": "insert"},
		},
		{
			"trucker_rows_written_total",
			map[string]string{"truck": "truck", "table": "public.whiskies", "operation": "update"},
		},
		{
			"trucker_query_mode_total",
			map[string]string{"truck": "truck", "side": "writer", "mode": "values", "adapter": "postgres"},
		},
	} {
		assertMetricPositive(t, text, tc.name, tc.labels)
	}

	// Replication lag gauge must exist (value >= 0).
	assertMetricExists(t, text, "trucker_replication_lag_bytes",
		map[string]string{"input_db": "pg_input_conn", "truck": "truck"})

	// Reader-side query mode must have been recorded (backfill uses the reader).
	assertMetricPositive(t, text, "trucker_query_mode_total",
		map[string]string{"truck": "truck", "side": "reader", "mode": "values", "adapter": "postgres"})

	// Per-changeset latency histograms must have observed at least one sample.
	assertHistogramHasSamples(t, text, "trucker_transaction_duration_seconds",
		map[string]string{"truck": "truck"})
	assertHistogramHasSamples(t, text, "trucker_writer_query_duration_seconds",
		map[string]string{"truck": "truck"})
}

// ---------- helpers ----------

func fetchMetrics(t *testing.T) string {
	t.Helper()
	resp, err := http.Get(metricsURL)
	if err != nil {
		t.Fatalf("GET %s: %v", metricsURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics body: %v", err)
	}
	return string(body)
}

// findMetricLine returns the first metrics text line that matches name and all
// required label key=value pairs, plus its numeric sample value.
func findMetricLine(text, name string, labels map[string]string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		allMatch := true
		for k, v := range labels {
			if !strings.Contains(line, k+`="`+v+`"`) {
				allMatch = false
				break
			}
		}
		if !allMatch {
			continue
		}
		// The sample value is the last whitespace-delimited token.
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		return val, true
	}
	return 0, false
}

func assertMetric(t *testing.T, text, name string, labels map[string]string, wantVal float64, exact bool) {
	t.Helper()
	val, ok := findMetricLine(text, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found in /metrics output", name, labels)
		return
	}
	if exact && val != wantVal {
		t.Errorf("metric %s%v: want %.0f, got %.0f", name, labels, wantVal, val)
	}
}

func assertMetricPositive(t *testing.T, text, name string, labels map[string]string) {
	t.Helper()
	val, ok := findMetricLine(text, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found in /metrics output", name, labels)
		return
	}
	if val <= 0 {
		t.Errorf("metric %s%v: want > 0, got %v", name, labels, val)
	}
}

func assertMetricExists(t *testing.T, text, name string, labels map[string]string) {
	t.Helper()
	_, ok := findMetricLine(text, name, labels)
	if !ok {
		t.Errorf("metric %s%v not found in /metrics output", name, labels)
	}
}

// assertHistogramHasSamples checks that the _count suffix of a histogram is > 0.
func assertHistogramHasSamples(t *testing.T, text, name string, labels map[string]string) {
	t.Helper()
	assertMetricPositive(t, text, name+"_count", labels)
}

// waitFor polls fn until it returns true or the deadline is exceeded.
func waitFor(t *testing.T, timeout time.Duration, fn func() bool) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within %s", timeout)
}
