SELECT COALESCE(r.id, r.old__id) AS id,
       r.name,
       {{ .operation | eq "delete" }} AS deleted
FROM {{ .rows }};
