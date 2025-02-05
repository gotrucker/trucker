TRUNCATE DATABASE trucker;

CREATE TABLE whiskies_flat (
  id String,
  name AggregateFunction(argMax, String, DateTime64),
  age AggregateFunction(argMax, Int32, DateTime64),
  type AggregateFunction(argMax, String, DateTime64),
  country AggregateFunction(argMax, String, DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_whiskies_flat AS
SELECT id,
       argMaxMerge(name) AS name,
       argMaxMerge(age) AS age,
       argMaxMerge(type) AS type,
       argMaxMerge(country) AS country
FROM whiskies_flat
GROUP BY id;
