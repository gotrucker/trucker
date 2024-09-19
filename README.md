# trucker

# Objective
Pick up data changes from postgres logical replication stream and allow getting
the data, possibly enriched by an extra query to the database, optionally
transform it, then output to some datastore (for now, the same or another
postgres db)

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

# Questions


# Scratchpad
