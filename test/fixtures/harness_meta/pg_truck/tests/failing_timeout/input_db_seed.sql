CREATE TABLE public.widgets (
    id integer PRIMARY KEY,
    name text NOT NULL
);
ALTER TABLE public.widgets REPLICA IDENTITY FULL;

-- An extra table that the truck does NOT replicate. It is never the truck's input table,
-- so the pre-test cleanup leaves it in place; IF NOT EXISTS keeps reruns idempotent.
CREATE TABLE IF NOT EXISTS public.unwatched (
    id bigserial PRIMARY KEY,
    note text
);

INSERT INTO public.widgets (id, name) VALUES (1, 'seed one');
