-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Table definitions for a very simple star schema (one dimension and one fact table).

-- Reset schema
-- ------------

DROP SCHEMA IF EXISTS star CASCADE;
CREATE SCHEMA star;

SET search_path = 'star';

-- Fact table and data
-- -------------------

CREATE TABLE ft (k INT, v INT NOT NULL);
CREATE INDEX ft_i1 ON ft(k);
CREATE INDEX ft_i2 ON ft(v, k);


INSERT INTO ft VALUES
  -- one NULL row in ft
  (NULL, 0),
  -- 1 and 2 have 2 rows each in ft
  (1, 1), (1, 1),
  (2, 2), (2, 2),
  (3, 3),
  (4, 4),
  -- mixed row for 5
  (5, 5), (NULL, 5)
  -- 7 is not present in either table
  ;

-- Dimension table and data (d1)
-- -----------------------------

CREATE TABLE d1 (k INT, v INT NOT NULL);
CREATE INDEX d1_i1 ON d1(k);
CREATE INDEX d1_i2 ON d1(v, k);

INSERT INTO d1 VALUES
  (NULL, 0), (NULL, 0), -- two NULL rows in d1
  -- 1 not present in d1
  (2, 2), (2, 2),
  (3, 3),
  -- 4 has no rows in d1
  (5, 5),
  (6, 6)
  -- 7 is not present in either table
  ;

-- Dimension table and data (d2)
-- -----------------------------

CREATE TABLE d2 (k INT, v INT NOT NULL);

INSERT INTO d2 VALUES
  (NULL, 0),
  -- 1 not present in d2
  (2, 2),
  (3, 3), (3, 3),
  (4, 4),
  (NULL, 5)
  -- 6 has no rows in d2
  -- 7 is not present in either table
  ;

-- Dimension table and data (d3)
-- -----------------------------

CREATE TABLE d3 (k INT, v INT NOT NULL);
CREATE INDEX d3_i1 ON d3(k);

INSERT INTO d3 VALUES
  (NULL, 0),
  -- 1 not present in d3
  (NULL, 2),
  (3, 3), (3, 3),
  -- 4 has no rows in d3
  (5, 5),
  (6, 6), (6, 6)
  -- 7 is not present in either table
  ;

-- PK materialized views
-- ---------------------

CREATE MATERIALIZED VIEW ft_pk AS
SELECT DISTINCT ON (k) k, v FROM ft WHERE k IS NOT NULL;

CREATE MATERIALIZED VIEW d1_pk AS
SELECT DISTINCT ON (k) k, v FROM d1 WHERE k IS NOT NULL;

CREATE MATERIALIZED VIEW d2_pk AS
SELECT DISTINCT ON (k) k, v FROM d2 WHERE k IS NOT NULL;

CREATE MATERIALIZED VIEW d3_pk AS
SELECT DISTINCT ON (k) k, v FROM d3 WHERE k IS NOT NULL;
