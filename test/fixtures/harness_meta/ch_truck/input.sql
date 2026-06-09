SELECT COALESCE(r.id, r.old__id) AS id,
       COALESCE(r.name, r.old__name) AS name,
       ({{ .operation | eq "delete" }})::int AS deleted
FROM {{ .rows }};
