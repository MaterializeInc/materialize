CREATE SCHEMA inventory;

grant create on schema inventory to postgres;

CREATE TABLE inventory.foo (a int, b int, primary key (a));
