INSERT INTO {{ .Output.Table  }} (id, name, age, type, country)
VALUES (
  {{ .Selected.id }},
  {{ .Selected.name }},
  {{ .Selected.age }} * 2,
  {{ .Selected.type }},
  {{ .Selected.country }}
);
