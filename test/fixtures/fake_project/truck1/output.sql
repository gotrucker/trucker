INSERT INTO public.whiskies_flat (id, name, age, type, country)
SELECT r.id, r.name, r.age * 2, r.type, r.country
FROM {{ .rows }} r;
