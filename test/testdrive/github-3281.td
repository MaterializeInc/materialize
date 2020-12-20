# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/3281.
#
# Verifies that the precedence of `->>`, `::`, and `CAST` are not mishandled
# when rendering a SQL statement.

# With the `--validate-catalog` option, which is enabled in CI, Testdrive
# will verify that views can be persisted and reloaded from the catalog, so the
# very presence of this once-buggy view is a sufficient test.

> CREATE MATERIALIZED VIEW v AS
  SELECT CAST (column1::jsonb->>'arrival_time' AS timestamptz) arrival_time
  FROM (VALUES ('{"arrival_time":"2020-05-04T13:07:41-04:00"}')) _

> SELECT * FROM v
"2020-05-04 17:07:41 UTC"
