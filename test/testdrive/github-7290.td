# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ file-append path=posix.txt
foo
bar

> CREATE MATERIALIZED SOURCE posix
  FROM FILE '${testdrive.temp-dir}/posix.txt'
  FORMAT BYTES;

> SELECT data FROM posix ORDER BY mz_line_no;
foo
bar

$ file-append path=non_posix.txt trailing-newline=false
foo
bar

> CREATE MATERIALIZED SOURCE non_posix
  FROM FILE '${testdrive.temp-dir}/non_posix.txt'
  FORMAT BYTES;

> SELECT data FROM non_posix ORDER BY mz_line_no;
foo
bar
