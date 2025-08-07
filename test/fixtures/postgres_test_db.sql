DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
DELETE FROM pg_publication;
DROP PUBLICATION IF EXISTS trucker_trucker;
DROP PUBLICATION IF EXISTS trucker_trucker_2;
SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots;
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;

CREATE TYPE age_category AS ENUM ('young', 'middle-aged', 'old');

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
  category age_category DEFAULT 'young',
  whisky_type_id int NOT NULL REFERENCES whisky_types(id)
);

CREATE TABLE public.more_whiskies (LIKE public.whiskies INCLUDING ALL);

CREATE TABLE public.whiskies_flat (
  id int PRIMARY KEY,
  name text NOT NULL,
  age int,
  type text,
  country text
);

CREATE TABLE public.weird_types (
  a_number bigint,
  a_bool boolean,
  a_date date,
  an_ip_addr inet,
  a_jsonb jsonb,
  a_ts timestamp,
  a_text_array text[]
);

ALTER TABLE public.countries REPLICA IDENTITY FULL;
ALTER TABLE public.whisky_types REPLICA IDENTITY FULL;
ALTER TABLE public.whiskies REPLICA IDENTITY FULL;
ALTER TABLE public.more_whiskies REPLICA IDENTITY FULL;

INSERT INTO public.countries (name)
VALUES ('Portugal'), ('Scotland'), ('Ireland'), ('Japan'), ('USA');

INSERT INTO public.whisky_types (name, country_id)
VALUES ('Bourbon', 5), ('Japanese', 4), ('Triple Distilled', 3), ('Single Malt', 2), ('Salty', 1);

INSERT INTO public.whiskies (name, age, whisky_type_id)
VALUES ('Glenfiddich', 15, 4), ('Lagavulin', 12, 3), ('Hibiki', 17, 2), ('Laphroaig', 10, 5);

INSERT INTO public.more_whiskies (name, age, whisky_type_id)
VALUES ('Jameson', 20, 4), ('Monkey Shoulder', 25, 3);

INSERT INTO public.weird_types (a_number, a_bool, a_date, an_ip_addr, a_jsonb, a_ts, a_text_array)
VALUES (1234567890, true, '2020-01-01', '192.168.0.1', '{"key": "value"}', '2020-01-01T00:37:00Z', '{a, b, c}'),
       (null, null, null, null, null, null, null);
