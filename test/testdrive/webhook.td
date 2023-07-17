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

> CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE '1'));

> CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK
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

> CREATE SOURCE webhook_json_with_headers IN CLUSTER webhook_cluster FROM WEBHOOK
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

> CREATE SOURCE webhook_bytes IN CLUSTER webhook_cluster FROM WEBHOOK
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

> CREATE SOURCE webhook_bytes_with_validation IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT BYTES
  VALIDATE USING ( headers->'signature' = 'test' );

$ webhook-append database=materialize schema=public name=webhook_bytes_with_validation signature=test
123

$ webhook-append database=materialize schema=public name=webhook_bytes_with_validation signature=invalid status=400
456

> CREATE SOURCE webhook_bytes_with_hmac IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT BYTES
  VALIDATE USING (
    decode(headers->'x-signature', 'base64') = hmac('body=' || convert_from(body, 'utf-8'), 'test_key', 'sha256')
  );

$ webhook-append name=webhook_bytes_with_hmac x-signature=HA0rQdPkCiNNNAladA0eTI8x5WZp5z8rBawQHiywznI=
hello world

$ webhook-append name=webhook_bytes_with_hmac x-signature=1cDmmXBhApqXZebb2u6WtdwHc2UtkMf7N11Zjk66wzo=
another_request

$ webhook-append name=webhook_bytes_with_hmac status=400
did_not_include_necessary_header

> SELECT * FROM webhook_bytes_with_hmac;
"hello world"
"another_request"

> CREATE SOURCE webhook_buildkite IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  VALIDATE USING (
    decode(split_part(headers->'x-buildkite-signature', 'signature::', 2), 'hex')
      =
    hmac(split_part(split_part(headers->'x-buildkite-signature', 'timestamp::', 2), ',', 1) || '.' || convert_from(body, 'utf-8'), 'test_key', 'sha256')
  );

$ webhook-append name=webhook_buildkite x-buildkite-signature=timestamp::42,signature::b610a43432fe965eb8e2a3ce4939a6bafaad3f35583c596e2f7271125a346d95
i hope this works

> SELECT * FROM webhook_buildkite;
"i hope this works"

> CREATE SOURCE webhook_hex IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  VALIDATE USING ( decode(convert_from(body, 'utf-8'), 'hex') = '\x42' );

$ webhook-append name=webhook_hex status=400
# 'z' is an invalid character in hex which causes an evaluation failure.
z

# Enable unstable dependencies so we can use mz_panic.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unstable_dependencies = true;

# Create a source that will panic when we run validation. Validation panicking should not take down
# all of environmentd, we should catch the panic.
#
# Note: if you change the message in the panic, then you need to update ci_logged_errors_detect.py.
> CREATE SOURCE webhook_validation_panic IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  VALIDATE USING ( mz_internal.mz_panic('webhook panic test') = headers::text );

$ webhook-append name=webhook_validation_panic status=503
abc

# Turn off the feature.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_webhook_sources = false

# Appending should now fail because the feature isn't enabled.
$ webhook-append database=materialize schema=public name=webhook_text status=400
d

# Cleanup.
DROP CLUSTER webhook_cluster CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unstable_dependencies = false;
