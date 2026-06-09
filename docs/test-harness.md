# Test Harness

The test harness runs one truck through the normal backfill, bounded catchup, and stream processing primitives against isolated test databases.

## CLI

```bash
trucker -gen test <truck> <test_name>
trucker -test run
trucker -test run <truck>
trucker -test run <truck> <test_name>
```

Normal production usage is unchanged:

```bash
trucker
trucker <project_path>
```

## Layout

```text
project/
├── trucker.yml
└── my_truck/
    ├── truck.yml
    ├── input.sql
    ├── output.sql
    └── tests/
        └── basic_insert/
            ├── input_db_seed.sql
            ├── output_db_seed.sql
            ├── stream_statements.sql
            └── expectations.sql
```

`input_db_seed.sql` runs against the input test DB before backfill. `output_db_seed.sql` runs against the output test DB before backfill. `stream_statements.sql` runs against the input test DB after backfill. `expectations.sql` runs against the output test DB after bounded catchup and the output LSN barrier complete.

## Test Databases

The harness does not add YAML keys. It clones the configured connections and appends `_test` to each configured database name. For example, `database: webapp` becomes `webapp_test`.

If the test DB does not exist, the harness creates it through the maintenance database: `postgres` for PostgreSQL and `default` for ClickHouse. Test DBs are not dropped after a run so failures can be inspected. Before each test, the harness drops user tables and Trucker replication artifacts inside the `_test` DB.

## Expectations

Every statement in `expectations.sql` is executed and all failures are reported. A statement passes only if it returns at least one row and every cell in every row is truthy.

Truthy values are SQL boolean `true` and integer `1`. Everything else fails, including `NULL`, `false`, `0`, strings like `'true'`, and strings like `'1'`.

## Required Privileges

The credentials in `trucker.yml` need these privileges on the test servers:

- `CREATE DATABASE` on PostgreSQL input/output servers and the equivalent on ClickHouse output servers.
- `CREATE` and `DROP` on objects inside the derived `_test` databases.
- `REPLICATION` on the PostgreSQL input user.

## Limitations

The v1 harness runs one truck per test and uses a fixed 30 second timeout. It does not isolate `panic` or `log.Fatal` exits in subprocesses, so severe setup/runtime errors may still terminate the test command.

## Developing the harness

The harness has its own self-test suite under `pkg/testharness/`:

- **Unit tests** (no database): statement splitting, truthiness classification, and the
  output-LSN barrier poll loop. These run as part of `make test`.
- **Golden fixtures** (`make test-harness`, guarded by the `harness` build tag): a single
  meta-project under `test/fixtures/harness_meta/` with one Postgres truck and one
  ClickHouse truck. Each `tests/<case>/` directory ships an `expected.json` describing the
  outcome, and `pkg/testharness/meta_test.go` runs the real harness against each case and
  asserts the `Result` matches.

The negative fixtures (`failing_wrong_count`, `failing_zero_rows`, `failing_bad_stream_sql`,
`failing_bad_seed_sql`, `failing_timeout`) are the important ones: they prove the harness
fails the *right* thing for the *right* reason — phase and reason — rather than merely
failing. `idempotent_rerun` runs twice in a row to prove the preserved `_test` databases
stay usable across runs.

Run the full self-test suite with:

```bash
make test-harness
```

`expected.json` fields: only `status` (`pass` / `fail`) is required. `phase`, `statement`,
and `reason` are matched only when present.
