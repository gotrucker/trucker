INSERT INTO public.whiskies_flat_b (id, name, age, type, country)
SELECT id, name, age * 2, type, country
FROM {{ .rows }}
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    age = EXCLUDED.age,
    type = EXCLUDED.type,
    country = EXCLUDED.country;
