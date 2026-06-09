INSERT INTO widgets_flat (id, name, deleted, version)
SELECT id, name, toUInt8(deleted), now64(9)
FROM {{ .rows }}
