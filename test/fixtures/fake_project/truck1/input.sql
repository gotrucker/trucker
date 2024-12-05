SELECT r.id,
       r.name,
       COALESCE(r.age::int, 0) - COALESCE(r.old__age::int, 0) AS age, -- FIXME: take care of correly typing NULL values in the VALUES literal. given that types from wal2json don't quite match up with postgres and Go types, can we even do this?
       t.name type,
       c.name country
FROM {{ .rows }}
JOIN public.whisky_types t ON r.whisky_type_id = t.id
JOIN public.countries c ON c.id = t.country_id;
