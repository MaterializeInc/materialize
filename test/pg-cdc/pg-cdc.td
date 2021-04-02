# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE numbers_${testdrive.seed} (number int PRIMARY KEY, is_prime bool, name text);
ALTER TABLE numbers_${testdrive.seed} REPLICA IDENTITY FULL;
CREATE PUBLICATION mz_source_${testdrive.seed} FOR ALL TABLES;
INSERT INTO numbers_${testdrive.seed} VALUES (5, true, 'five');

## CREATE with correct information should pass purification and fail afterwards.

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE "numbers_${testdrive.seed}"
Postgres sources not yet supported

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE numbers_${testdrive.seed} ()
Postgres sources not yet supported

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE "numbers_${testdrive.seed}" (number int NOT NULL, is_prime bool NULL, name text NULL)
Postgres sources not yet supported

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE "numbers_${testdrive.seed}" (number int NOT NULL, is_prime bool, name text)
Postgres sources not yet supported

## CREATE with incorrect information should fail purification.

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE numbers_${testdrive.seed} (number int NOT NULL)
incorrect column specification: 1 columns were specified, upstream has 3: number, is_prime, name

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE numbers_${testdrive.seed} (number int NOT NULL, is_prime bool null, name text, extra numeric NULL)
incorrect column specification: 4 columns were specified, upstream has 3: number, is_prime, name

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE numbers_${testdrive.seed} (number int NOT NULL, is_prime int, name text)
incorrect column specification: specified column does not match upstream source, specified: is_prime int4 NULL, upstream: is_prime bool NULL

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE "numbers_${testdrive.seed}" (number int NULL, is_prime bool null, name text)
incorrect column specification: specified column does not match upstream source, specified: number int4 NULL, upstream: number int4 NOT NULL

! CREATE MATERIALIZED SOURCE "numbers"
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLE numbers_${testdrive.seed} (number int NOT NULL, is_prime bool, name text NOT NULL)
incorrect column specification: specified column does not match upstream source, specified: name text NOT NULL, upstream: name text NULL

## CREATE SOURCES

! CREATE SOURCES
  FROM POSTGRES
    HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
    PUBLICATION 'mz_source_${testdrive.seed}'
    NAMESPACE 'public'
    TABLES ("numbers_${testdrive.seed}" AS "numbers", numbers_${testdrive.seed} AS "second_numbers" (number int NOT NULL, is_prime bool NULL, name text NULL))
CREATE SOURCES not yet supported
