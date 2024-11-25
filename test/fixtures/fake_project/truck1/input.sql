SELECT r.id, r.name, r.age, t.name type, c.name country -- these also exist: old_id, old_name, old_age, old_type, old_country, operation
FROM {{ .rows }} r
JOIN whisky_types t ON r.whisky_type_id = t.id
JOIN countries c ON c.id = t.country_id;
