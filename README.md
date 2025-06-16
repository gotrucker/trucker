# Trucker

[![License](https://img.shields.io/github/license/tonyfg/trucker)](LICENSE)

Trucker is a SQL-based streaming ETL tool that reads from a database replication stream, enriches replicated changes by querying and JOINing other data from the source database, then writes the result to another database (or another table in the same database).

Having very low latency, Trucker is ideal for implementing:
- Real-time analytics
- Calculating and storing aggregated data for interactive applications and dashboards
- Replacing database triggers with something that runs after a transaction has been committed
- Incremental materialized views
- Simplifying complex data pipelines

## Features

- **SQL-based**: Define reads/transformations/writes using familiar SQL syntax
- **Low latency**: Data changes are read, processed and written in near real-time
- **Flexible transformations**: Enrich data by joining with other tables
- **Configuration as code**: Simple and templateable YAML files for configuration. Makes it easy to use configurations and secrets from orchestrators like Kubernetes.
- **Transactional processing**: Process database transactions atomically (on databases that support it)

## Supported Databases

| Database   | Reading | Writing |
| ---------- | ------- | ------- |
| PostgreSQL | Yes     | Yes     |
| Clickhouse | No      | Yes     |

## Installation

### Docker

```bash
docker pull tonyfg/trucker:latest
```

### Binary Releases

Download pre-built binaries from the [releases page](https://github.com/tonyfg/trucker/releases).

### Build from Source

```bash
git clone https://github.com/tonyfg/trucker.git
cd trucker
go build
```

## Quick Start

1. Create a directory for your trucker project
2. Define your database connections in `trucker.yml` (TODO: link to example)
3. Create a folder for each data pipeline
4. Add a truck.yml file to define where to read data from and where to write to
5. Define your input and output operations with SQL (input.sql and output.sql files)
6. Run Trucker pointing to your project directory

```bash
trucker /path/to/your/project
```

## Configuration

### Project Structure

```
my-project/
├── trucker.yml             # Database connection definitions
├── pipeline1/          # Each folder defines a data pipeline
│   ├── truck.yml       # Input/output configuration
│   ├── input.sql       # SQL for reading/enriching data
│   └── output.sql      # SQL for writing data to destination database
└── pipeline2/
    └── ...
```

### Connection Configuration (trucker.yml)

```yaml
connections:
  - name: webapp_db
    adapter: postgres
    host: pg.example.org
    port: 5432
    database: my_app_db
    user: db_user
    pass: db_password
    
  - name: analytics_db
    adapter: clickhouse
    host: clickhouse.example.org
    database: my_analytics
    user: db_user
    pass: db_password
```

### Pipeline Configuration (truck.yml)

```yaml
input:
  connection: webapp_db
  table: public.users

output:
  connection: analytics_db
```

## SQL Examples

### Input SQL (input.sql)

```sql
SELECT
  r.id,
  r.name,
  r.email,
  u.last_login_at,
  COUNT(o.id) AS order_count
FROM {{ .rows }}
LEFT JOIN user_stats u ON r.id = u.user_id
LEFT JOIN orders o ON r.id = o.user_id
GROUP BY r.id, r.name, r.email, u.last_login_at
```

### Output SQL (output.sql)

```sql
INSERT INTO analytics.user_metrics (
  id, 
  name, 
  email, 
  last_login_at, 
  order_count
)
SELECT id, name, email, last_login_at, order_count
FROM {{ .rows }}
```

## Observability

Not implemented yet. Trucker will provide observability capabilities through:
- Prometheus compatible metrics for monitoring trucker and pipelines
- Structured logging via [zap](https://pkg.go.dev/go.uber.org/zap)

## Documentation

TODO
For detailed documentation, visit [Trucker Documentation](https://github.com/tonyfg/trucker/wiki).

## Projects you may find useful

- [pgstream](https://github.com/xataio/pgstream)
- [pgdeltastream](https://github.com/hasura/pgdeltastream)
- [PeerDB](https://www.peerdb.io/)
- [pg_flo](https://www.pgflo.io/)
- [BemiDB](https://github.com/BemiHQ/BemiDB)
- [Materialize](https://materialize.com/)
- [Feldera](https://feldera.com/)
- [Striim](https://www.striim.com/)
- [ElectricSQL](https://github.com/electric-sql/electric)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Roadmap
- Base documentation and examples
- Database migrations system
- Structured logging
- Prometheus compatible metrics exporter
- trucker.yml/truck.yml options to deal with special backfill situations (whether to truncate destination tables, etc)
- MySQL/MariaDB support
- Snowflake support
- AWS Redshift support
- Test harness for testing data pipelines in isolation
- Motherduck support
- (maybe) Google bigquery support
- Transactional consistency enhancements
- Column filtering for performance optimization
- Integrate DuckDB as a library for expanded read/write support
- Support for other sources/destinations of data (webhooks, S3, etc)
- Comprehensive e2e testing with TPC-DS datasets
