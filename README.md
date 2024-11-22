# trucker

# Objective
Pick up data changes from postgres logical replication stream and allow getting
the data, possibly enriched by an extra query to the database, optionally
transform it, then output to some datastore (for now, the same or another
postgres db)

# Similar projects
- pgstream: https://github.com/xataio/pgstream
- pgdeltastream: https://github.com/hasura/pgdeltastream
- PeerDB: https://www.peerdb.io/
- pg_flo: https://www.pgflo.io/
- BemiDB: https://github.com/BemiHQ/BemiDB
- Materialize
- Feldera
- Striim
- Check this out for more ideas: https://github.com/DataExpert-io/data-engineer-handbook

# How to define a data pipeline
- Each pipeline has it's own folder with some files inside
- config file with source db/table config + destination db config

- trucker.json: contains data source definitions. templatable
|- folder1
|- folder2
|- folderN: each folder contains a data pipeline definition
   |- config.json: defines trigger (input) connection/table and output connection/table
   |- transform.sql: Optional. Runs for every operation. 
   |                 Allows transforming the input into a suitable format for output, can query from other tables in DB.
   |                 Templatable, "operation", "new" and "old" variables are available.
   |                 SELECT column names must match column names in output table
   |- insert.sql: optional, like transform.sql but only runs on insert                 
   |- update.sql: optional, like transform.sql but only runs on updates                 
   |- delete.sql: optional, like transform.sql but only runs on deletes                 

insert.sql/update.sql/delete.sql run after transform.sql if it exists. they have access to a "transformed" variable in those cases.


# trucker.yml example
- trucker.yml is templatable to make it possible to inject variables/secrets.
  connections:
  - name: webapp_db
    adapter: postgres
    host: awesome-db-master.example.org
    port: 5432 # optional, default 5432
    database: a_cool_db
    user: a_db_user
    pass: the_db_password
    replica_host: awesome-db-master.example.org # optional, if set master will only be used to create replication slots and publications. streaming changes and queries will run on replica
    replica_port: 5432 # optional, default 5432
    replica_db: a_cool_db_replica # optional, defaults to same as the master db
    replica_user: a_db_user_replica # optional, defaults to same as the master db
    replica_pass: the_replica_password # optional, defaults to same as the master db
      
  - name: warehouse
    adapter: postgres
    host_path: /run/secrets/warehouse_host
    port_path: /run/secrets/warehouse_port
    database_path: /run/secrets/warehouse_database
    user_path: ...
    pass_path: ...

  - name: realtime_analytics
    adapter: clickhouse
    host: {{ env("CLICKHOUSE_HOST") }}
    port: ...


# Steps
- create if not exists replication slot
- create/update publication. does this need to be done before or after the replication slot is setup?
- backfill, and also select the current WAL position
  - transactionally correct backfill: https://stackoverflow.com/questions/69459481/retrieve-lsn-of-postgres-database-during-transaction
  - if connected to master server: pg_current_wal_lsn()
  - if connected to replica server: pg_last_wal_receive_lsn()
- set replication slot to point to the WAL position we got from above

# Logging
https://pkg.go.dev/go.uber.org/zap

# OpenTelemetry / Prometheus metrics
?

# Wishes for the future
- Integrate DuckDB as a library to allow having lots more input / output sources

# Questions


# Scratchpad
## Backfill
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
      - FUCK! how do we know when we've caught up?
      - We need to find a way to measure the distance, and when we're close we can just reboot all the trucks so we use a single replication stream.
      - trucks that are a bit ahead can ignore events with a lower LSN than their destination LSN until everyone's in sync.
## WAL2JSON messages
### Insert
Without identity replica = full: {"change":[{"kind":"insert","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[7,"a"]},{"kind":"insert","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[8,"b"]},{"kind":"insert","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[9,"c"]}]}
With identity replica = full:    {"change":[{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"a",12,1]},{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"b",15,2]},{"kind":"insert","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"c",18,3]}]}
### Update
Without identity replica = full: {"change":[{"kind":"update","schema":"public","table":"countries","columnnames":["id","name"],"columntypes":["integer","text"],"columnvalues":[6,"boda"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[6]}}]}
With identity replica = full:    {"change":[{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[1,"boda1",15,4],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"Glenfiddich",15,4]}},{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[3,"boda3",12,1],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"a",12,1]}},{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[4,"boda4",15,2],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"b",15,2]}},{"kind":"update","schema":"public","table":"whiskies","columnnames":["id","name","age","whisky_type_id"],"columntypes":["integer","text","integer","integer"],"columnvalues":[5,"boda5",18,3],"oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"c",18,3]}}]}
### Delete
Without identity replica = full: {"change":[{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[3]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[4]}},{"kind":"delete","schema":"public","table":"whisky_types","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[5]}}]}
With identity replica = full:    {"change":[{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[1,"boda1",15,4]}},{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[3,"boda3",12,1]}},{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[4,"boda4",15,2]}},{"kind":"delete","schema":"public","table":"whiskies","oldkeys":{"keynames":["id","name","age","whisky_type_id"],"keytypes":["integer","text","integer","integer"],"keyvalues":[5,"boda5",18,3]}}]}
