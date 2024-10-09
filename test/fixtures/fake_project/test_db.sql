CREATE TABLE public.countries (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE public.whisky_types (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  country_id INTEGER NOT NULL REFERENCES countries(id)
);

CREATE TABLE public.whiskies (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER NOT NULL,
  whisky_type_id INTEGER NOT NULL REFERENCES whisky_types(id)
);

CREATE TABLE public.whiskies_flat (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER NOT NULL,
  type TEXT NOT NULL,
  country TEXT NOT NULL
);

INSERT INTO public.countries (name)
VALUES ('Portugal'), ('Scotland'), ('Ireland'), ('Japan'), ('USA');

INSERT INTO public.whisky_types (name, country_id)
VALUES ('Bourbon', 5), ('Japanese', 4), ('Triple Distilled', 3), ('Single Malt', 2), ('Salty', 1);

INSERT INTO public.whiskies (name, age, whisky_type_id)
VALUES ('Glenfiddich', 15, 4);
