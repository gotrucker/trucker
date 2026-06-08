# Metrics

Trucker exposes a Prometheus-compatible metrics endpoint at `/metrics`. It provides observability into pipeline throughput, latency, errors, and replication health.

## Enabling

Set `metrics_addr` in `trucker.yml`:

```yaml
connections:
  - name: webapp_db
    adapter: postgres
    host: pg.example.org
    port: 5432
    # ...

metrics_addr: ":9091"
```

The metrics HTTP server starts on the configured port. If `metrics_addr` is empty or absent, no metrics server is started.

## Metric Reference

### Counters

| Metric | Labels | Description |
| ------ | ------ | ----------- |
| `trucker_transactions_total` | `truck`, `input_db`, `output_db` | Number of WAL transactions processed |
| `trucker_rows_read_total` | `truck`, `table`, `operation` | Rows returned by the input SQL query |
| `trucker_rows_written_total` | `truck`, `table`, `operation` | Rows written to the output |
| `trucker_slow_queries_total` | `truck`, `side` | Queries exceeding the slow query threshold |
| `trucker_autoadvance_total` | `truck` | WAL commits with no rows for this truck (LSN advanced without a write) |
| `trucker_query_mode_total` | `truck`, `side`, `mode`, `adapter` | Queries using VALUES inline vs temporary table |
| `trucker_db_errors_total` | `adapter`, `side` | Database errors in reader/writer operations |
| `trucker_truck_panics_total` | `truck` | Unhandled panics in truck goroutines |
| `trucker_replication_errors_total` | `input_db`, `kind` | Fatal errors in the replication stream reader |
| `trucker_replication_restarts_total` | `input_db` | Replication stream connection resets |

### Histograms

| Metric | Labels | Description |
| ------ | ------ | ----------- |
| `trucker_transaction_duration_seconds` | `truck`, `input_db`, `output_db` | Total time to process a WAL transaction end-to-end |
| `trucker_reader_query_duration_seconds` | `truck`, `table`, `operation` | Time spent running the input SQL query per changeset |
| `trucker_writer_query_duration_seconds` | `truck`, `table`, `operation` | Time spent running the output SQL query per changeset |

### Gauges

| Metric | Labels | Description |
| ------ | ------ | ----------- |
| `trucker_replication_lag_bytes` | `input_db`, `truck` | WAL bytes the truck writer is behind the reader |
| `trucker_replication_lag_seconds` | `input_db`, `truck` | Seconds since the WAL commit being processed was first seen by the reader |
| `trucker_transactions_in_flight` | `truck` | Transactions currently being processed by the truck |
| `trucker_lsn_flush_pending` | `truck` | Whether there is a pending deferred LSN flush (0 or 1) |
| `trucker_build_info` | `version` | Static build information (always 1) |

## Label Reference

| Label | Description |
| ----- | ----------- |
| `truck` | Pipeline directory name |
| `input_db` | Name of the input connection from `trucker.yml` |
| `output_db` | Name of the output connection from `trucker.yml` |
| `table` | Source table name (e.g. `public.whiskies`) |
| `operation` | SQL operation: `insert`, `update`, `delete` |
| `side` | Pipeline side: `reader` or `writer` |
| `adapter` | Database adapter: `postgres` or `clickhouse` |
| `mode` | Query execution mode: `values` or `temporary_table` |
| `kind` | Error kind: `connection`, `protocol`, `parse` |
| `version` | Trucker build version |

## Example Prometheus Configuration

```yaml
scrape_configs:
  - job_name: trucker
    static_configs:
      - targets:
          - trucker_host:9091
```

## Example Grafana Queries

**Transaction rate per pipeline:**
```
rate(trucker_transactions_total[1m])
```

**P99 transaction processing latency:**
```
histogram_quantile(0.99, rate(trucker_transaction_duration_seconds_bucket[5m]))
```

**Rows written per minute:**
```
rate(trucker_rows_written_total[1m])
```

**Replication lag in bytes:**
```
trucker_replication_lag_bytes
```

**Replication lag in seconds:**
```
trucker_replication_lag_seconds
```

**Top slowest pipelines (reader side):**
```
topk(5, trucker_slow_queries_total)
```

**Database errors rate:**
```
rate(trucker_db_errors_total[5m])
```
