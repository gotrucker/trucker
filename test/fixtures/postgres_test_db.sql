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

ALTER TABLE public.countries REPLICA IDENTITY FULL;
ALTER TABLE public.whisky_types REPLICA IDENTITY FULL;
ALTER TABLE public.whisky_types REPLICA IDENTITY FULL;

INSERT INTO public.countries (name)
VALUES ('Portugal'), ('Scotland'), ('Ireland'), ('Japan'), ('USA');

INSERT INTO public.whisky_types (name, country_id)
VALUES ('Bourbon', 5), ('Japanese', 4), ('Triple Distilled', 3), ('Single Malt', 2), ('Salty', 1);

INSERT INTO public.whiskies (name, age, whisky_type_id)
VALUES ('Glenfiddich', 15, 4), ('Lagavulin', 12, 3), ('Hibiki', 17, 2), ('Laphroaig', 10, 5);
