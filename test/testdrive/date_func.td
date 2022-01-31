# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Argument to EXTRACT not checked for correctness, NULL returned if invalid #5521
# expr: propagate errors when reducing date_{part,trunc} #5700
#

! SELECT EXTRACT(NULL FROM CAST('2011-11-11' AS DATE));
contains:unit 'null' not recognized

! SELECT EXTRACT(NULL FROM CAST('11:11:11' AS TIME));
contains:unit 'null' not recognized

! SELECT EXTRACT(NULL FROM CAST('2011-11-11' AS TIMESTAMP));
contains:unit 'null' not recognized

! SELECT EXTRACT(NULL FROM CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
contains:unit 'null' not recognized

# NULL is an argument here, so the function returns NULL
> SELECT DATE_PART(NULL, CAST('2011-11-11' AS DATE)) IS NULL;
true

> SELECT DATE_PART(NULL, CAST('11:11:11' AS TIME)) IS NULL;
true

> SELECT DATE_PART(NULL, CAST('2011-11-11' AS TIMESTAMP)) IS NULL;
true

> SELECT DATE_PART(NULL, CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE)) IS NULL;
true

! SELECT EXTRACT('foo' FROM CAST('2011-11-11' AS DATE));
contains:unit 'foo' not recognized

! SELECT EXTRACT('foo' FROM CAST('11:11:11' AS TIME));
contains:unit 'foo' not recognized

! SELECT EXTRACT('foo' FROM CAST('2011-11-11' AS TIMESTAMP));
contains:unit 'foo' not recognized

! SELECT EXTRACT('foo' FROM CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
contains:unit 'foo' not recognized


! SELECT DATE_PART('foo', CAST('2011-11-11' AS DATE));
contains:unit 'foo' not recognized

! SELECT DATE_PART('foo', CAST('11:11:11' AS TIME));
contains:unit 'foo' not recognized

! SELECT DATE_PART('foo', CAST('2011-11-11' AS TIMESTAMP));
contains:unit 'foo' not recognized

! SELECT DATE_PART('foo', CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
contains:unit 'foo' not recognized


! SELECT EXTRACT('' FROM CAST('2011-11-11' AS DATE));
contains:unit '' not recognized

! SELECT EXTRACT('' FROM CAST('11:11:11' AS TIME));
contains:unit '' not recognized

! SELECT EXTRACT('' FROM CAST('2011-11-11' AS TIMESTAMP));
contains:unit '' not recognized

! SELECT EXTRACT('' FROM CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
contains:unit '' not recognized


! SELECT DATE_PART('', CAST('2011-11-11' AS DATE));
contains:unit '' not recognized

! SELECT DATE_PART('', CAST('11:11:11' AS TIME));
contains:unit '' not recognized

! SELECT DATE_PART('', CAST('2011-11-11' AS TIMESTAMP));
contains:unit '' not recognized

! SELECT DATE_PART('', CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
contains:unit '' not recognized


> SELECT EXTRACT('second' FROM CAST('2011-11-11' AS DATE));
contains:unit '' not recognized

> SELECT EXTRACT('second' FROM CAST('2011-11-11' AS TIMESTAMP));
0

> SELECT EXTRACT('second' FROM CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
0

> SELECT DATE_PART('second', CAST('2011-11-11' AS DATE));
0

> SELECT DATE_PART('second', CAST('2011-11-11' AS TIMESTAMP));
0

> SELECT DATE_PART('second', CAST('2011-11-11' AS TIMESTAMP WITH TIME ZONE));
0

> SELECT EXTRACT('day' FROM CAST(NULL AS DATE)) IS NULL;
true

> SELECT EXTRACT('day' FROM CAST(NULL AS TIME)) IS NULL;
true

> SELECT EXTRACT('day' FROM CAST(NULL AS TIMESTAMP)) IS NULL;
true

> SELECT EXTRACT('day' FROM CAST(NULL AS TIMESTAMP WITH TIME ZONE)) IS NULL;
true

> SELECT DATE_PART('day', CAST(NULL AS DATE)) IS NULL;
true

> SELECT DATE_PART('day', CAST(NULL AS TIME)) IS NULL;
true

> SELECT DATE_PART('day', CAST(NULL AS TIMESTAMP)) IS NULL;
true

> SELECT DATE_PART('day', CAST(NULL AS TIMESTAMP WITH TIME ZONE)) IS NULL;
true
