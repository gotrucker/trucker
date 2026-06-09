CREATE TABLE public.widgets (
    id integer PRIMARY KEY,
    name text NOT NULL
);
ALTER TABLE public.widgets REPLICA IDENTITY FULL;

-- "nope" is not a column on public.widgets, so this seed must fail in the input_seed phase.
INSERT INTO public.widgets (id, nope) VALUES (1, 'seed one');
