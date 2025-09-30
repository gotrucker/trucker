-- we don't actually need to write anything
INSERT INTO public.double_countries (id, name)
SELECT id, name
FROM {{ .rows }}
