# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=125ms

#
# Test the ENCODE and DECODE functions
# Additional tests available at test/sqllogictest/encode.slt
#

# Nulls

> SELECT ENCODE(NULL, 'hex') IS NULL
true

> SELECT DECODE(NULL, 'hex') IS NULL
true

# Empty input -> empty output

> SELECT ENCODE('', 'hex') = '';
true

> SELECT ENCODE('', 'base64') = '';
true

> SELECT DECODE('', 'hex') = '\x';
true

# Invalid encoding
! SELECT ENCODE('abc', 'foo');
invalid encoding name 'foo'

! SELECT DECODE('abc', 'foo');
invalid encoding name 'foo'

# Invalid input

! SELECT DECODE('ZZ', 'hex')
invalid hexadecimal digit: "Z"

! SELECT DECODE('AZ', 'hex')
invalid hexadecimal digit: "Z"

! SELECT DECODE('0x00', 'hex');
nvalid hexadecimal digit: "x"

! SELECT DECODE('|', 'base64');
invalid base64 end sequence

! SELECT DECODE('A|', 'base64');
invalid base64 end sequence

! SELECT DECODE('AA|', 'base64');
invalid base64 end sequence

! SELECT DECODE('AAA|', 'base64');
invalid symbol "|" found while decoding base64 sequence

! SELECT DECODE('YWJjZGVmCg=', 'base64');
unexpected "=" while decoding base64 sequence

! SELECT DECODE('\', 'escape');
invalid input syntax for type bytea: ends with escape character: "\"

! SELECT DECODE('\\\', 'escape');
invalid input syntax for type bytea: ends with escape character: "\\\"

! SELECT DECODE('\9', 'escape');
invalid input syntax for type bytea: invalid escape sequence: "\9"

! SELECT DECODE('\99', 'escape');
invalid input syntax for type bytea: invalid escape sequence: "\99"

! SELECT DECODE('\999', 'escape');
invalid input syntax for type bytea: invalid escape sequence: "\999"

# the null byte

> SELECT DECODE('\000', 'escape');
\\x00

> SELECT DECODE('00', 'hex');
\\x00

> SELECT DECODE('AA==', 'base64');
\\x00

> SELECT ENCODE(DECODE('\000', 'escape'), 'hex');
00

# > SELECT ENCODE(DECODE('\000', 'escape'), 'escape');
# 00

> SELECT ENCODE(DECODE('\000', 'escape'), 'base64');
AA==

# Actual useful input

> SELECT ENCODE('abcd', 'hex');
61626364

> SELECT DECODE('AABBaaBB', 'hex') = '\xaabbaabb';
true

> SELECT ENCODE('\\EnglishБългарски', 'escape') = '\\English\320\221\321\212\320\273\320\263\320\260\321\200\321\201\320\272\320\270';
true

> SELECT ENCODE('abc', 'base64');
YWJj

> SELECT ENCODE('абц', 'base64');
0LDQsdGG

> SELECT DECODE('\\\000\111', 'escape') = '\x5c0049';
true

> SELECT DECODE('YWJjZGVm', 'base64') = 'abcdef';
true

#
# Make sure the functions also work in a view context
#

> CREATE TABLE t1 (f1 bytea, f2 string);
> CREATE MATERIALIZED VIEW v1 AS SELECT ENCODE(f1, 'hex'), DECODE(f2, 'hex') FROM t1;
> SHOW CREATE VIEW v1;
"materialize.public.v1" "CREATE VIEW \"materialize\".\"public\".\"v1\" AS SELECT \"pg_catalog\".\"encode\"(\"f1\", \'hex\'), \"pg_catalog\".\"decode\"(\"f2\", \'hex\') FROM \"materialize\".\"public\".\"t1\""

> INSERT INTO t1 VALUES ('ab', ENCODE('ab', 'hex'));

> SELECT * FROM v1;
"6162" "ab"
