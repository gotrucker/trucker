SELECT r.id, r.name, coalesce(r.age, 0) - coalesce(r.old__age, 0), t.name type, c.name country -- these also exist: old_id, old_name, old_age, old_type, old_country, operation
FROM {{ .rows }}
JOIN public.whisky_types t ON r.whisky_type_id = t.id
JOIN public.countries c ON c.id = t.country_id;
