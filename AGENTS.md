## Project Overview

Trucker is a SQL-based streaming ETL tool that reads from PostgreSQL replication streams, enriches data through SQL transformations, and writes results to PostgreSQL or ClickHouse databases. It provides real-time, low-latency data processing with transactional consistency.

## Development Commands

### Development Environment

**Always run `make dev` before executing any development commands.** The entire development environment runs inside Docker Compose — the Go toolchain, databases, and ClickHouse are all containers. Commands run on the host without the containers will fail or produce incorrect results.

- `make dev` - Start development environment (run this first, every time)
- `make stop` - Stop all Docker Compose services
- `make sh` - Access Go development container shell
- `make clean` - Clean up all Docker containers and volumes

### Building and Testing

**Always use Makefile targets instead of crafting raw commands.** When a Makefile target exists for a task, use it. Only drop to `docker compose exec go <cmd>` for one-off commands that don't have a recipe yet, and consider adding a recipe if the command is likely to be used again.

**All `go` commands must run inside the `go` container**, never on the host. The host may have a different Go version or lack the required environment variables and network access to the test databases.

- `make test` - Run all tests (`go test -p 1 ./...` inside the `go` container)
- `make test-gnarly` - Run the TPC-DS gnarly test suite (slow, ~15 min first run)
- `make fmt` - Format all Go source files

For commands not yet in the Makefile, run them via:
```
docker compose exec go go <args>
```

It's important to use `-p 1` when running `go test` to avoid conflicting data in the test DB.

### Docker Images
- `make build_images` - Build Docker image for current platform
- `make push_images` - Build and push multi-platform images (requires git tag)

## Architecture Overview

### Core Components

**Main Entry Point (`main.go`)**
- Accepts project directory path as argument (defaults to current directory)
- Initializes signal handling and orchestrates truck lifecycle

**Configuration System (`pkg/config/`)**
- `trucker.yml` - Database connection definitions at project root
- `truck.yml` - Per-pipeline configuration in subdirectories
- `input.sql` / `output.sql` - SQL transformation files per pipeline

**Main Orchestration (`pkg/mainroutines/`)**
- Groups trucks by input database connection
- Manages PostgreSQL replication clients per connection
- Orchestrates three-phase execution: backfill → catchup → streaming

**Truck Engine (`pkg/truck/`)**
- Individual data pipeline processor
- Coordinates Reader → SQL transformation → Writer flow
- Handles transaction-level processing with configurable slow query thresholds

**Database Adapters**
- `pkg/postgres/` - PostgreSQL logical replication (read/write), backfill, SQL transformations
- `pkg/clickhouse/` - ClickHouse write operations and SQL value conversions
- `pkg/db/` - Common interfaces and types

### Processing Flow

1. **Initialization**: Load configuration, create replication clients per input connection
2. **Backfill**: Snapshot existing data with transactional consistency
3. **Catchup**: Process replication log from backfill LSN to current position
4. **Streaming**: Continuously process new replication changes

### PostgreSQL Replication Integration

- Uses logical replication with the `pgoutput` plugin (PostgreSQL's native binary protocol v2 with `streaming='true'` for in-progress large transactions)
- Manages publication/subscription and replication slot lifecycle
- Processes INSERT/UPDATE/DELETE operations with full row data when `REPLICA IDENTITY FULL`

### Project Structure Pattern

```
project/
├── trucker.yml           # Database connections
├── pipeline1/           # Data pipeline directory
│   ├── truck.yml       # Pipeline config (input/output connections)
│   ├── input.sql       # Data enrichment SQL (optional)
│   └── output.sql      # Write destination SQL
└── pipeline2/
    └── ...
```

## Testing Strategy

- **Unit tests**: `*_test.go` files alongside source code
- **System tests**: `test/system/` - Full pipeline tests with real databases
- **Integration tests**: `test/integration/` - Component integration testing
- **Test fixtures**: `test/fixtures/` - Sample project configurations and SQL schemas

Test database containers are managed via Docker Compose with PostgreSQL primary/replica setup and ClickHouse.

## Important Implementation Notes

- SQL templating uses `{{ .rows }}` placeholder for streaming data
- Transaction-level processing maintains ACID properties where supported
- LSN (Log Sequence Number) tracking ensures exactly-once delivery
- Supports both master and replica PostgreSQL connections for reading
