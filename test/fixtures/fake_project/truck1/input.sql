SELECT {{ .New.id }} id,
       {{ .New.name }} name,
       {{ .New.age }} age,
       t.name type,
       c.name country
FROM whisky_types t
JOIN countries c ON c.id = t.country_id
WHERE t.id = {{ .New.whisky_type_id }};
