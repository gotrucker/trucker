TRUNCATE DATABASE trucker;

CREATE TABLE whiskies_flat (
  id String,
  name AggregateFunction(argMax, String, DateTime64),
  age AggregateFunction(argMax, Int32, DateTime64),
  type AggregateFunction(argMax, String, DateTime64),
  country AggregateFunction(argMax, String, DateTime64),
  deleted AggregateFunction(argMax, Boolean, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_whiskies_flat AS
SELECT * FROM (
  SELECT id,
         argMaxMerge(name) AS name,
         argMaxMerge(age) AS age,
         argMaxMerge(type) AS type,
         argMaxMerge(country) AS country,
         argMaxMerge(deleted) AS deleted
  FROM whiskies_flat
  GROUP BY id
) WHERE NOT deleted;
