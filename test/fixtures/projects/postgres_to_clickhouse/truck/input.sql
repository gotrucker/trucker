SELECT COALESCE(r.id, r.old__id) AS id,
       COALESCE(r.name, r.old__name) AS name,
       COALESCE(r.age, 0) - COALESCE(r.old__age, 0) AS age,
       COALESCE(t.name, '') type,
       COALESCE(c.name, '') country
FROM {{ .rows }}
LEFT JOIN public.whisky_types t ON r.whisky_type_id = t.id
LEFT JOIN public.countries c ON c.id = t.country_id;
