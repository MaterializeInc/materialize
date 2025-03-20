# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW COLUMNS from test.tester.test_data_types
name               nullable  type                            comment
--------------------------------------------------------------------
c1                 true      boolean                         ""
c2                 true      smallint                        ""
c3                 true      integer                         ""
c4                 true      bigint                          ""
c5                 true      real                            ""
c6                 true      "double precision"              ""
c7                 true      date                            ""
c8                 true      "timestamp without time zone"   ""
c9                 true      "timestamp with time zone"      ""
c10                true      numeric                         ""
c11                true      bytea                           ""
c12                false     text                            mz_is_primary_key
c13                true      jsonb                           ""
_fivetran_deleted  true      boolean                         ""
_fivetran_synced   true      "timestamp with time zone"      ""

> SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 FROM test.tester.test_data_types
c1     c2  c3  c4  c5   c6   c7          c8                      c9                        c10  c11  c12  c13
-------------------------------------------------------------------------------------------------------------------------
false  1   2   3   4.5  6.7  2023-01-02  "2023-03-04 05:06:07"  "2023-03-04 05:06:07 UTC"  8.9  a    b    "{\"x\":\"y\"}"
