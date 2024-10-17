# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that multibyte characters are handled correctly
#
#

> CREATE TABLE ct (f1 CHAR(20));

> INSERT INTO ct VALUES ('това е текст');

> SELECT * FROM ct;
"това е текст        "

> CREATE TABLE vt (f1 VARCHAR(20));

> INSERT INTO vt VALUES ('това е текст');

> SELECT * FROM vt;
"това е текст"

> SELECT LENGTH(f1) FROM ct;
12

> SELECT OCTET_LENGTH(f1) FROM ct;
30

> SELECT LENGTH(f1) FROM vt;
12

> SELECT OCTET_LENGTH(f1) FROM vt;
22

! INSERT INTO vt VALUES ('това е текст това е текст');
contains:value too long for type character varying(20)

! INSERT INTO ct VALUES ('това е текст това е текст');
contains:value too long for type character(20)
