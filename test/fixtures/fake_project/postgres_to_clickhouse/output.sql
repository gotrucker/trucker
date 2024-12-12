INSERT INTO trucker.whiskies_flat (id, name, age, type, country)
SELECT id,
       argMaxState(tuple(name::Nullable(String)), now64()),
       argMaxState(tuple((age * 2)::Nullable(Int32)), now64()),
       argMaxState(tuple(type::Nullable(String)), now64()),
       argMaxState(tuple(country::Nullable(String)), now64())
FROM {{ .rows }}
GROUP BY id
