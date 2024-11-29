TRUNCATE DATABASE trucker;

CREATE TABLE whiskies_flat (
  id Int32,
  name String,
  age Int32,
  type String,
  country String
)
ENGINE = MergeTree()
ORDER BY id;
