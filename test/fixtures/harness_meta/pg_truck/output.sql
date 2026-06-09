INSERT INTO public.widgets_flat (id, name, deleted)
SELECT id, name, deleted
FROM {{ .rows }}
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    deleted = EXCLUDED.deleted;
