# Better logging
We need to have logging levels at the very least:
- WARN: Only startup/shutdown messages + stuff that is out of the ordinary
- INFO: WARN + 
- DEBUG: 

# Make input.sql optional
For simple pipelines we could allow feeding changes directly to output.sql.

# DB Migrations
We need to be able to run migrations on output DBs to create/update the schema
before starting trucks. I guess it's fine to allow running migrations on input
DBs too...

We would have a "migrate" directory inside the project root, and for each DB we
want to create migrations for, there would be a subdirectory with the same name
as the connection inside trucker.yml. Migration files would simply be SQL files,
which we would run in a transaction (for DBs that support it):

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


# Test harness
We need to add the possibility for users to have tests for their trucks. These
could be organized inside a "test" directory inside the truck, with one
sub-directory for each test:

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


