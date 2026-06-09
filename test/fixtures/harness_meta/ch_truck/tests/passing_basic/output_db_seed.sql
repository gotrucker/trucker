CREATE TABLE widgets_flat (
    id Int32,
    name String,
    deleted UInt8,
    version DateTime64(9)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id;
