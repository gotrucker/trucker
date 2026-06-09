CREATE TABLE IF NOT EXISTS {{.InputTable}} (
    id integer PRIMARY KEY,
    name text NOT NULL
);
ALTER TABLE {{.InputTable}} REPLICA IDENTITY FULL;

INSERT INTO {{.InputTable}} (id, name) VALUES (1, 'Alice'), (2, 'Bob');
