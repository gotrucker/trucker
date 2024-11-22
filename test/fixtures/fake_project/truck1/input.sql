SELECT r.id, r.name, r.age, r.type, r.country -- these also exist: old_id, old_name, old_age, old_type, old_country
FROM {{ .Rows }} r -- also: .InsertedRows, .UpdatedRows, or .DeletedRows
JOIN countries c ON r.id = t.country_id
WHERE t.id = r.whisky_type_id;
