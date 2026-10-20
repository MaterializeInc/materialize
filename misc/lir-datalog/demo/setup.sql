-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Money transfers between accounts, and the accounts flagged for review.
CREATE TABLE transfers (id int8 NOT NULL, src int8 NOT NULL, dst int8 NOT NULL, amount int8 NOT NULL);
CREATE TABLE flagged (account int8 NOT NULL);

INSERT INTO transfers
SELECT i, (i * 7919) % 50000, (i * 104729) % 50000, i % 1000
FROM generate_series(1::int8, 50000) AS i;

INSERT INTO flagged
SELECT (i * 37) % 50000 FROM generate_series(1::int8, 1000) AS i;
