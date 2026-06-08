package metrics

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var latencyBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300}

var TransactionDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "trucker_transaction_duration_seconds",
		Help:    "Total time to process a WAL transaction end-to-end.",
		Buckets: latencyBuckets,
	},
	[]string{"truck", "input_db", "output_db"},
)

var ReaderQueryDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "trucker_reader_query_duration_seconds",
		Help:    "Time spent running the input SQL query per changeset.",
		Buckets: latencyBuckets,
	},
	[]string{"truck", "table", "operation"},
)

var WriterQueryDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "trucker_writer_query_duration_seconds",
		Help:    "Time spent running the output SQL query per changeset.",
		Buckets: latencyBuckets,
	},
	[]string{"truck", "table", "operation"},
)

var SlowQueries = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_slow_queries_total",
		Help: "Number of queries exceeding the slow query threshold.",
	},
	[]string{"truck", "side"},
)

var Transactions = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_transactions_total",
		Help: "Number of WAL transactions processed.",
	},
	[]string{"truck", "input_db", "output_db"},
)

var RowsRead = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_rows_read_total",
		Help: "Number of rows returned by the input SQL query.",
	},
	[]string{"truck", "table", "operation"},
)

var RowsWritten = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_rows_written_total",
		Help: "Number of rows written to the output.",
	},
	[]string{"truck", "table", "operation"},
)

var AutoAdvances = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_autoadvance_total",
		Help: "WAL commits with no rows for this truck (LSN advanced without a write).",
	},
	[]string{"truck"},
)

var QueryMode = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_query_mode_total",
		Help: "Number of queries using VALUES inline vs temporary table.",
	},
	[]string{"truck", "side", "mode", "adapter"},
)

var ReplicationLagBytes = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "trucker_replication_lag_bytes",
		Help: "WAL bytes the truck writer is behind the reader (clientXLogPos minus per-truck acked LSN).",
	},
	[]string{"input_db", "truck"},
)

var ReplicationLagSeconds = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "trucker_replication_lag_seconds",
		Help: "Seconds since the WAL commit being processed was first seen by the reader.",
	},
	[]string{"input_db", "truck"},
)

var DBErrors = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_db_errors_total",
		Help: "Database errors in reader/writer operations.",
	},
	[]string{"adapter", "side"},
)

var TruckPanics = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_truck_panics_total",
		Help: "Unhandled panics in truck goroutines.",
	},
	[]string{"truck"},
)

var ReplicationErrors = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_replication_errors_total",
		Help: "Fatal errors in the replication stream reader.",
	},
	[]string{"input_db", "kind"},
)

var ReplicationRestarts = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "trucker_replication_restarts_total",
		Help: "Number of replication stream connection resets.",
	},
	[]string{"input_db"},
)

var TransactionsInFlight = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "trucker_transactions_in_flight",
		Help: "Number of transactions currently being processed by the truck.",
	},
	[]string{"truck"},
)

var LsnFlushPending = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "trucker_lsn_flush_pending",
		Help: "Whether there is a pending deferred LSN flush waiting to be written (0 or 1).",
	},
	[]string{"truck"},
)

var BuildInfo = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "trucker_build_info",
		Help: "Static build information.",
	},
	[]string{"version"},
)

var registered bool

func Register(version string) {
	if registered {
		return
	}
	registered = true

	collectors := []prometheus.Collector{
		TransactionDuration,
		ReaderQueryDuration,
		WriterQueryDuration,
		SlowQueries,
		Transactions,
		RowsRead,
		RowsWritten,
		AutoAdvances,
		QueryMode,
		ReplicationLagBytes,
		ReplicationLagSeconds,
		DBErrors,
		TruckPanics,
		ReplicationErrors,
		ReplicationRestarts,
		TransactionsInFlight,
		LsnFlushPending,
		BuildInfo,
	}
	for _, c := range collectors {
		prometheus.MustRegister(c)
	}
	BuildInfo.WithLabelValues(version).Set(1)
}

func Serve(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[metrics] server error: %v", err)
		}
	}()
	return srv
}
