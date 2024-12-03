DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

CREATE TABLE public.countries (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name text NOT NULL
);

CREATE TABLE public.whisky_types (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name text NOT NULL,
  country_id int NOT NULL REFERENCES countries(id)
);

CREATE TABLE public.whiskies (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name text NOT NULL,
  age int NOT NULL,
  whisky_type_id int NOT NULL REFERENCES whisky_types(id)
);

CREATE TABLE public.whiskies_flat (
  id int PRIMARY KEY,
  name text NOT NULL,
  age int NOT NULL,
  type text NOT NULL,
  country text NOT NULL
);

CREATE TABLE public.weird_types (
  a_number bigint,
  a_bool boolean,
  a_date date,
  an_ip_addr inet,
  a_jsonb jsonb,
  a_text_arr text[],
  a_ts timestamp,
  an_uuid uuid
);

ALTER TABLE public.countries REPLICA IDENTITY FULL;
ALTER TABLE public.whisky_types REPLICA IDENTITY FULL;
ALTER TABLE public.whisky_types REPLICA IDENTITY FULL;

INSERT INTO public.countries (name)
VALUES ('Portugal'), ('Scotland'), ('Ireland'), ('Japan'), ('USA');

INSERT INTO public.whisky_types (name, country_id)
VALUES ('Bourbon', 5), ('Japanese', 4), ('Triple Distilled', 3), ('Single Malt', 2), ('Salty', 1);

INSERT INTO public.whiskies (name, age, whisky_type_id)
VALUES ('Glenfiddich', 15, 4), ('Lagavulin', 12, 3), ('Hibiki', 17, 2), ('Laphroaig', 10, 5);

INSERT INTO public.weird_types (a_number, a_bool, a_date, an_ip_addr, a_jsonb, a_text_arr, a_ts, an_uuid)
VALUES (1234567890, true, '2020-01-01', '192.168.0.1', '{"key": "value"}', ARRAY['a', 'b', 'c'], '2020-01-01T00:00:00Z', '123e4567-e89b-12d3-a456-426614174000'),
       (null, null, null, null, null, null, null, null);

INSERT INTO public.uuids_tbl (the_uuid)
VALUES ('123e4567-e89b-12d3-a456-426614174000'),
       ('123e4567-e89b-12d3-a456-426614174001'),
       ('123e4567-e89b-12d3-a456-426614174002');
