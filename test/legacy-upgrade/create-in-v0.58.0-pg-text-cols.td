# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

$ skip-if
SELECT mz_version_num() >= 6000;

> CREATE SECRET IF NOT EXISTS pgpass AS 'postgres'
> CREATE CONNECTION IF NOT EXISTS pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

# Create a table with a dangling text cols reference, which will be disallowed
# in newer versions of MZ. All we care is that the later versions boot with it.

> CREATE SOURCE dangling_text_cols_source
  FROM POSTGRES
  CONNECTION pgconn
  (
    PUBLICATION 'upgrade_pg_cdc_publication',
    TEXT COLUMNS (cdc_enum_table.a, another_enum_table.a)
  )
  FOR TABLES (
    cdc_enum_table AS cdc_enum_table_dangling_text_cols_source
  );
