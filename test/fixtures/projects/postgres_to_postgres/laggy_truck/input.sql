-- simulate a long-running query to delay truck processing
SELECT id,
       name || ' ' || name AS name,
       pg_sleep(1)
FROM {{ .rows }}
