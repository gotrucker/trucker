INSERT INTO {{.InputTable}} (id, name) VALUES (3, 'Charlie');
UPDATE {{.InputTable}} SET name = 'Robert' WHERE id = 2;
DELETE FROM {{.InputTable}} WHERE id = 1;
