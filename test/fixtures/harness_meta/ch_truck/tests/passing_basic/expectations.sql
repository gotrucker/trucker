-- ClickHouse expectations: (a = b) yields UInt8, which the truthiness classifier treats as
-- truthy when it equals 1. ReplacingMergeTree(version) + FINAL keeps the newest row per id.
SELECT (count() = 3) FROM widgets_flat FINAL;
SELECT (count() = 1) FROM widgets_flat FINAL WHERE id = 1 AND deleted = 1;
SELECT (count() = 1) FROM widgets_flat FINAL WHERE id = 2 AND name = 'seed two updated' AND deleted = 0;
SELECT (count() = 1) FROM widgets_flat FINAL WHERE id = 3 AND name = 'stream three' AND deleted = 0;
