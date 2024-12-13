INSERT INTO trucker.whiskies_flat (id, name, age, type, country)
SELECT r.id,
       argMaxState(tuple(r.name::Nullable(String)), now64()),
       argMaxState(tuple((r.age * 2)::Nullable(Int32)), now64()),
       argMaxState(tuple(r.type::Nullable(String)), now64()),
       argMaxState(tuple(r.country::Nullable(String)), now64())
FROM {{ .rows }}
GROUP BY id
