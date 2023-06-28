# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Exercies Webhook sources.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_webhook_sources = true

> CREATE SOURCE webhook_text FROM WEBHOOK
  BODY FORMAT TEXT;

> SHOW COLUMNS FROM webhook_text;
name   nullable  type
---------------------
body   false     text

$ webhook-append database=materialize schema=public name=webhook_text
a

$ webhook-append database=materialize schema=public name=webhook_text
b

$ webhook-append database=materialize schema=public name=webhook_text
c

> SELECT * FROM webhook_text;
a
b
c

> CREATE SOURCE webhook_json_with_headers FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADERS;

> SHOW COLUMNS FROM webhook_json_with_headers;
name     nullable  type
------------------------
body     false     jsonb
headers  false     map

$ webhook-append database=materialize schema=public name=webhook_json_with_headers content-type=application/json app=test_drive
{
  "hello": "world"
}

$ webhook-append database=materialize schema=public name=webhook_json_with_headers content-type=application/json app=other
{
  "goodbye": "world"
}

> SELECT body FROM webhook_json_with_headers WHERE headers -> 'app' = 'test_drive';
"{\"hello\":\"world\"}"

$ webhook-append database=materialize schema=public name=webhook_json_with_headers content-type=application/json app=
{
  "empty": "world"
}

> SELECT body FROM webhook_json_with_headers WHERE headers -> 'app' = '';
"{\"empty\":\"world\"}"

$ webhook-append database=materialize schema=public name=webhook_json_with_headers content-type=application/json app=list
[1, 2, 3]

> SELECT body FROM webhook_json_with_headers WHERE headers -> 'app' = 'list';
"[1,2,3]"

$ webhook-append database=materialize schema=public name=webhook_json_with_headers content-type=application/json app=list2
{ "foo": [1, 2, 3] }

> SELECT body FROM webhook_json_with_headers WHERE headers -> 'app' = 'list2';
"{\"foo\":[1,2,3]}"

$ webhook-append database=materialize schema=public name=webhook_json_with_headers content-type=application/json app=string
"Hellö String"

> SELECT body FROM webhook_json_with_headers WHERE headers -> 'app' = 'string';
"\"Hellö String\""

! SELECT * FROM webhook_json_with_headers;
contains: binary encoding of map types is not implemented

# An invalid body should return a 400.
$ webhook-append database=materialize schema=public name=webhook_json_with_headers status=400 content-type=application/json
invalid-json

# A source that doesn't exist should return a 404.
$ webhook-append database=materialize schema=public name=non_existent_source status=404
x

# Trying to append to an object that isn't a webhook should fail.
> CREATE TABLE not_a_webhook ( a int8 );
$ webhook-append database=materialize schema=public name=not_a_webhook status=404
d

> SELECT * FROM webhook_text;
a
b
c

> CREATE SOURCE webhook_bytes FROM WEBHOOK
  BODY FORMAT BYTES;

> SHOW COLUMNS FROM webhook_bytes;
name   nullable  type
---------------------
body   false     bytea

$ webhook-append database=materialize schema=public name=webhook_bytes
和製漢語

$ webhook-append database=materialize schema=public name=webhook_bytes
null

$ webhook-append database=materialize schema=public name=webhook_bytes
123

> SELECT * FROM webhook_bytes;
"\\xe5\\x92\\x8c\\xe8\\xa3\\xbd\\xe6\\xbc\\xa2\\xe8\\xaa\\x9e"
null
123

# Turn off the feature.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_webhook_sources = false

# Appending should now fail because the feature isn't enabled.
$ webhook-append database=materialize schema=public name=webhook_text status=400
d
