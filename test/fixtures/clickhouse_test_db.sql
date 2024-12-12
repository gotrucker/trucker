TRUNCATE DATABASE trucker;

CREATE TABLE whiskies_flat (
  id Int32,
  name AggregateFunction(argMax, Tuple(Nullable(String)), DateTime64),
  age AggregateFunction(argMax, Tuple(Nullable(Int32)), DateTime64),
  type AggregateFunction(argMax, Tuple(Nullable(String)), DateTime64),
  country AggregateFunction(argMax, Tuple(Nullable(String)), DateTime64)
)
ENGINE = AggregatingMergeTree
ORDER BY id;

CREATE VIEW v_whiskies_flat AS
SELECT id,
       argMaxMerge(name).1 AS name,
       argMaxMerge(age).1 AS age,
       argMaxMerge(type).1 AS type,
       argMaxMerge(country).1 AS country
FROM whiskies_flat
GROUP BY id;
