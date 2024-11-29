INSERT INTO trucker.whiskies_flat (id, name, age, type, country)
SELECT id, name, age * 2, type, country
FROM {{ .rows }};
