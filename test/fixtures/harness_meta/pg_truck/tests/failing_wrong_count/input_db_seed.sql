CREATE TABLE public.widgets (
    id integer PRIMARY KEY,
    name text NOT NULL
);
ALTER TABLE public.widgets REPLICA IDENTITY FULL;

INSERT INTO public.widgets (id, name) VALUES (1, 'seed one'), (2, 'seed two');
