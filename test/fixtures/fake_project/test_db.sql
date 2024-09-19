CREATE TABLE countries (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE whisky_types (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  country_id INTEGER NOT NULL REFERENCES countries(id)
);

CREATE TABLE whiskies (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER NOT NULL,
  whisky_type_id INTEGER NOT NULL REFERENCES whisky_types(id)
);

CREATE TABLE whiskies_flat (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER NOT NULL,
  type TEXT NOT NULL,
  country TEXT NOT NULL
);

INSERT INTO countries (name)
VALUES ('Portugal'), ('Scotland'), ('Ireland'), ('Japan'), ('USA');

INSERT INTO whisky_types (name, country_id)
VALUES ('Bourbon', 5), ('Japanese', 4), ('Triple Distilled', 3), ('Single Malt', 2), ('Salty', 1);

INSERT INTO whiskies (name, age, whisky_type_id)
VALUES ('Glenfiddich', 15, 4);
