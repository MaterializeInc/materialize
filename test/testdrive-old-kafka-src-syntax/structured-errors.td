# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Testing user-focused error messages

> CREATE SECRET pgpass AS 'postgres'
! CREATE CONNECTION pg TO POSTGRES (
    HOST 'localhost',
    PORT 5433,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
contains:error connecting to server:

> CREATE MATERIALIZED VIEW mv as SELECT 1;

# test that `hint:` works!
! DROP VIEW mv
contains:is not a view
hint:Use DROP MATERIALIZED VIEW to remove a materialized view.
