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

CREATE TABLE ft (k INT, v INT NOT NULL, fk1 INT, fk2 INT);
CREATE INDEX ft_i1 ON ft(k);

INSERT INTO ft VALUES
  -- one NULL row in ft
  (NULL, 100, 1, 2),
  -- 1-5 have one row each
  (1, 101, 1, 2),
  (2, 102, 2, 3),
  (3, 103, 3, 4),
  (4, 104, 4, 5),
  (5, 105, 5, 6)
  -- 7 is not present in either table
  ;

-- Dimension table and data (d1)
-- -----------------------------

CREATE TABLE d1 (pk1 INT, pk2 INT NOT NULL, v INT NOT NULL);
CREATE INDEX d1_i1 ON d1(pk1, pk2);

INSERT INTO d1 VALUES
  -- pk1 != pk2 rows
  (NULL, 0, 0),
  (0, 0, 1),
  -- 1 not present in d1
  (2, 3, 0), (3, 2, 1),
  (3, 4, 0),
  -- 4 has no rows in d1
  (5, 6, 0), (6, 5, 1),
  (6, 7, 0),

  -- pk1 = pk2 rows
  (3, 3, 0),
  (5, 5, 0), (5, 5, 1)
  ;

-- Dimension table and data (d2)
-- -----------------------------

CREATE TABLE d2 (pk1 INT, pk2 INT NOT NULL, v INT NOT NULL);
CREATE INDEX d2_i1 ON d2(pk1, pk2);

INSERT INTO d2 VALUES
  -- pk1 != pk2 rows
  (NULL, 0, 0),
  (1, 2, 0), (2, 1, 0),
  (2, 3, 0),
  (3, 4, 0), (4, 3, 1),
  (4, 5, 0),
  (NULL, 5, 0),
  -- 6 has no rows in d2

  -- pk1 = pk2 rows
  (3, 3, 0), (3, 3, 1),
  (4, 4, 0)
  ;

-- Dimension table and data (d3)
-- -----------------------------

CREATE TABLE d3 (pk1 INT, pk2 INT, v INT NOT NULL);
CREATE INDEX d3_i1 ON d3(pk1, pk2);

INSERT INTO d3 VALUES
  -- pk1 != pk2 rows
  (0, 0, 1),
  -- 1 not present in d3
  (NULL, 2, 0),
  (3, 4, 0), (4, 3, 1),
  -- 4 has no rows in d3
  (5, 6, 0),
  (6, 7, 0), (7, 6, 1),

  -- pk1 = pk2 rows
  (NULL, NULL, 0),
  (3, 3, 0),
  (4, 4, 1),
  (5, 5, 0), (5, 5, 1)
  ;

-- PK materialized views
-- ---------------------

CREATE MATERIALIZED VIEW ft_pk AS
SELECT DISTINCT ON (k) k, v FROM ft;

CREATE MATERIALIZED VIEW d1_pk AS
SELECT DISTINCT ON (pk1, pk2) pk1, pk2, v FROM d1;

CREATE MATERIALIZED VIEW d2_pk AS
SELECT DISTINCT ON (pk1, pk2) pk1, pk2, v FROM d2;

CREATE MATERIALIZED VIEW d3_pk AS
SELECT DISTINCT ON (pk1, pk2) pk1, pk2, v FROM d3;
