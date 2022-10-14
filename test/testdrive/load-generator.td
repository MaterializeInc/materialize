# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SOURCE auction_house FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

$ set-regex match=\d replacement=<SIZE>

> SHOW SOURCES
accounts      subsource      <null>
auction_house load-generator <SIZE>
auctions      subsource      <null>
bids          subsource      <null>
organizations subsource      <null>
users         subsource      <null>

$ unset-regex

> CREATE CONNECTION IF NOT EXISTS kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

# Validate that the ID column of the load generator data is usable as a key.
> CREATE SINK accounts_sink FROM accounts
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-accounts-${testdrive.seed}')
  KEY (id)
  FORMAT JSON
  ENVELOPE UPSERT;
