# TODO

## Existing TODOs

- Test connection to RDS with rds.force_ssl=1
- Test input from partitioned table

- Test postgres passwords with the following characters:
  - %
  - &
  - ?

## Database Connectivity

- Possibility to force SSL on db connections

## Better Logging
https://github.com/uber-go/zap

We need to have logging levels at the very least:
- WARN: Only startup/shutdown messages + stuff that is out of the ordinary
- INFO: WARN + 
- DEBUG: 

## Feature Enhancements

- Make input.sql optional
  For simple pipelines we could allow feeding changes directly to output.sql.

- Transactional consistency (each run of input/output should process a transaction in one go)
- Add only_columns and except_columns to truck.yml input section for performance improvement
- When people use old__*, check if postgres tables are set to REPLICA IDENTITY FULL (or equivalent for other DBs). Show a decent error msg and exit if it's not
- Integrate DuckDB as a library to allow having lots more input / output sources
- Large tests with TPC-DS dataset and some gnarly scenarios

## DB Migrations

We need to be able to run migrations on output DBs to create/update the schema
before starting trucks. I guess it's fine to allow running migrations on input
DBs too...

We would have a "migrate" directory inside the project root, and for each DB we
want to create migrations for, there would be a subdirectory with the same name
as the connection inside trucker.yml. Migration files would simply be SQL files,
which we would run in a transaction (for DBs that support it):

```
my-trucker-project
| - trucker.yml
| - trucks
|   | ...
| - migrate
    | - my_output_db
    |   | - 2024-12-25T17:31:54_down.sql
    |   | - 2024-12-25T17:31:54_up.sql
    |   | - 2025-01-05T11:01:05_down.sql
    |   | - 2025-01-05T11:01:05_up.sql
    | - my_output_db_schema.sql
    | - another_db
    |   | - 2025-02-12T09:17:22_down.sql
    |   | - 2025-02-12T09:17:22_up.sql
    | - another_db_schema.sql
```

## Test Harness

We need to add the possibility for users to have tests for their trucks. These
could be organized inside a "test" directory inside the truck, with one
sub-directory for each test:

```
my-truck
| - truck.yml
| - input.yml
| - output.yml
| - test
    | - my-amazing-test <- the test directory
    |   | - backfill.sql <- create input table, and add pre-backfill data to it. this will run before the truck is started
    |   | - stream.sql <- any INSERT/UPDATE/DELETEs to run on the input DB after the backfill is done
    |   | - test.sql <- A query that runs on the output DB. Can return any number of rows with a single column with a TRUE value. Any rows with FALSE, or zero rows returned is considered a test failure.
    | - another-amazing-test 
        | - backfill.sql
        | - stream.sql
        | - test.sql
```

## Backfill Implementation Details

- Group trucks by input connection
- For every truck in each group
  - Check if destination table filled in / destination LSN tracking is up
    - Read destination LSN into a struct so we can update replication slot to the oldest LSN among all trucks
  - If there's no LSN, then we need a backfill regardless of whether we're replicating from a table that was already a part of the publication or not
  - Collect whether we need to:
    - add input table to publication
    - create a temporary replication slot + snapshot for backfill
  - group trucks inside group into trucks that can continue streaming vs trucks that need backfill
    - This can be 2 slices of truck structs
    - Start goroutine that reads from replication stream and publishes to truck input channels (will send to trucks in the streaming truck slice)
      - Can't move replication slot to an LSN that's after the backfill snapshot LSN. Can still update destination LSNs on the destination databases.
    - Start goroutine to backfill each truck that needs backfilling. Once backfill is done for everyone, open a second replication stream and stream from the LSN onwards until we catch up with the other trucks.
      - How do we know when we've caught up?
      - We need to find a way to measure the distance, and when we're close we can just reboot all the trucks so we use a single replication stream.
      - trucks that are a bit ahead can ignore events with a lower LSN than their destination LSN until everyone's in sync.

## WAL2JSON Message Examples

### Insert Messages

Without identity replica = full:
```json
{"change":[{"kind":"insert","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[7,"a"]},{"kind":"insert","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[8,"b"]},{"kind":"insert","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[9,"c"]}]}
```

With identity replica = full:
```json
{"change":[{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"a",12,1]},{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"b",15,2]},{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"c",18,3]}]}
```

### Update Messages

Without identity replica = full:
```json
{"change":[{"kind":"update","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[6,"boda"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[6]}}]}
```

With identity replica = full:
```json
{"change":[{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[1,"boda1",15,4],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"Glenfiddich",15,4]}},{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"boda3",12,1],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"a",12,1]}},{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"boda4",15,2],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"b",15,2]}},{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"boda5",18,3],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"c",18,3]}}]}
```

### Delete Messages

Without identity replica = full:
```json
{"change":[{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[3]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[4]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[5]}}]}
```

With identity replica = full:
```json
{"change":[{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"boda1",15,4]}},{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"boda3",12,1]}},{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"boda4",15,2]}},{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"boda5",18,3]}}]}
```

## Implementation Notes

### Steps for Initialization
- Create if not exists replication slot
- Create/update publication (determine if this needs to be done before or after the replication slot is setup)
- Backfill, and also select the current WAL position
  - Transactionally correct backfill: https://stackoverflow.com/questions/69459481/retrieve-lsn-of-postgres-database-during-transaction
  - If connected to master server: pg_current_wal_lsn()
  - If connected to replica server: pg_last_wal_receive_lsn()
- Set replication slot to point to the WAL position we got from above

### Release Process
```
git tag v0.x -am "Version 0.x"
make build_images push_images
```
