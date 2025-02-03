INSERT INTO trucker.whiskies_flat (id, name, age, type, country)
SELECT r.id,
       argMaxState(r.name, now64()),
       argMaxState((r.age * 2)::Int32, now64()),
       argMaxState(r.type, now64()),
       argMaxState(r.country, now64())
FROM {{ .rows }}
GROUP BY id
