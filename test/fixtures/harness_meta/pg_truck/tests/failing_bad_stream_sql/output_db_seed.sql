CREATE TABLE public.widgets_flat (
    id integer PRIMARY KEY,
    name text,
    deleted boolean NOT NULL DEFAULT false
);
