# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default replicas=1

# Exercises Webhook sources.

> CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE '1'));

> CREATE CLUSTER webhook_compute REPLICAS (r1 (SIZE '1'));

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

> CREATE VIEW webhook_text_ascii_code AS SELECT ascii(body) FROM webhook_text WHERE ascii(body) % 2 = 0;

> SELECT * FROM webhook_text_ascii_code;
98

> ALTER SOURCE webhook_text RENAME TO webhook_text_renamed;

! SELECT * FROM webhook_text;
contains: unknown catalog item 'webhook_text'

$ webhook-append database=materialize schema=public name=webhook_text status=404
d

> SELECT * FROM webhook_text_renamed;
a
b
c

$ webhook-append database=materialize schema=public name=webhook_text_renamed
d

> SELECT * FROM webhook_text_renamed;
a
b
c
d

> SELECT * FROM webhook_text_ascii_code;
98
100

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
  CHECK (
    WITH (HEADERS)
    headers->'signature' = 'test'
  );

$ webhook-append database=materialize schema=public name=webhook_bytes_with_validation signature=test
123

$ webhook-append database=materialize schema=public name=webhook_bytes_with_validation signature=invalid status=400
456

> CREATE SOURCE webhook_bytes_with_hmac IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT BYTES
  CHECK (
    WITH (HEADERS, BODY)
    decode(headers->'x-signature', 'base64') = hmac('body=' || body, 'test_key', 'sha256')
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

> CREATE SECRET webhook_secret AS 'shared_key';

> CREATE SOURCE webhook_bytes_with_secret IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  CHECK (
    WITH ( HEADERS, BODY BYTES, SECRET webhook_secret BYTES )
    decode(headers->'x-signature', 'base64') = hmac(body, webhook_secret, 'sha256')
  )

$ webhook-append name=webhook_bytes_with_secret x-signature=VNCe6bTKrlFO46GfiUYR/xFpeZ2H/KbLfR9oJKYAwkc=
using an mz secret

> SELECT * FROM webhook_bytes_with_secret;
"using an mz secret"

> CREATE SOURCE webhook_buildkite IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  CHECK (
    WITH (HEADERS, BODY)
    decode(split_part(headers->'x-buildkite-signature', 'signature::', 2), 'hex')
      =
    hmac(split_part(split_part(headers->'x-buildkite-signature', 'timestamp::', 2), ',', 1) || '.' || body, 'test_key', 'sha256')
  );

$ webhook-append name=webhook_buildkite x-buildkite-signature=timestamp::42,signature::b610a43432fe965eb8e2a3ce4939a6bafaad3f35583c596e2f7271125a346d95
i hope this works

> SELECT * FROM webhook_buildkite;
"i hope this works"

> CREATE SOURCE webhook_hex IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  CHECK ( WITH (BODY) decode(body, 'hex') = '\x42' );

$ webhook-append name=webhook_hex status=400
# 'z' is an invalid character in hex which causes an evaluation failure.
z

# Can use SECRETs as both Bytes and Strings.

> CREATE SECRET webhook_secret_bytes AS 'this_key_is_bytes';

> CREATE SOURCE webhook_double_validation IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  CHECK (
    WITH (
      HEADERS,
      BODY BYTES,
      SECRET webhook_secret,
      SECRET webhook_secret_bytes BYTES
    )
    decode(headers->'x-signature-sha256', 'hex') = hmac(convert_from(body, 'utf-8'), webhook_secret, 'sha256')
      AND
    decode(headers->'x-signature-md5', 'hex') = hmac(body, webhook_secret_bytes, 'md5')
  )

$ webhook-append name=webhook_double_validation x-signature-sha256=20460da764521c155989f9ede00d6047c459c87bca6712eef27f72ae32c62d3f x-signature-md5=c34fd128f787067796212d31fced1881
materialize space monkey

> SELECT * FROM webhook_double_validation;
"materialize space monkey"

# Webhooks should support special characters like a /

> CREATE SOURCE "webhook_with_/" IN CLUSTER webhook_cluster FROM WEBHOOK BODY FORMAT TEXT;

$ webhook-append name=webhook_with_/ status=404
wont_work

$ webhook-append name=webhook_with_%2F
will_work

> SELECT * FROM "webhook_with_/"
"will_work"

> CREATE SOURCE webhook_text_headers_block_list IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  INCLUDE HEADERS (NOT 'accept', NOT 'host')

$ webhook-append name=webhook_text_headers_block_list
foo

> SELECT body, headers::text FROM webhook_text_headers_block_list
foo "{content-length=>3}"

> CREATE SOURCE webhook_text_headers_allow_list IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  INCLUDE HEADERS ('x-random')

$ webhook-append name=webhook_text_headers_allow_list x-random=bar
anotha_one

> SELECT body, headers::text FROM webhook_text_headers_allow_list
anotha_one "{x-random=>bar}"

> CREATE SOURCE webhook_text_filtering_headers IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  INCLUDE HEADER 'x-timestamp' as event_timestamp
  INCLUDE HEADER 'x-id' as event_id
  INCLUDE HEADERS (NOT 'accept', NOT 'content-length', NOT 'host', NOT 'x-api-key')
  CHECK ( WITH (HEADERS) headers->'x-api-key' = 'abc123' )

$ webhook-append name=webhook_text_filtering_headers x-timestamp=100 x-id=a x-api-key=abc123 content-type=text/example
request_1

$ webhook-append name=webhook_text_filtering_headers x-api-key=wrong_key status=400
request_bad

$ webhook-append name=webhook_text_filtering_headers x-api-key=abc123 x-random=foo
request_missing_some_mapped_headers

> SELECT body, headers::text, event_timestamp, event_id FROM webhook_text_filtering_headers
request_1 "{content-type=>text/example,x-id=>a,x-timestamp=>100}" 100 a
request_missing_some_mapped_headers "{x-random=>foo}" <null> <null>

> CREATE SOURCE webhook_with_time_based_rejection IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT
  CHECK (
    WITH (HEADERS)
    (headers->'timestamp'::text)::timestamp + INTERVAL '30s' >= now()
  )

$ webhook-append name=webhook_with_time_based_rejection timestamp=2020-01-01 status=400
this_will_get_rejected

$ set-from-sql var=current_ts
SELECT now()::text

$ webhook-append name=webhook_with_time_based_rejection timestamp=${current_ts}
this_will_work

> SELECT body FROM webhook_with_time_based_rejection
this_will_work

# Unnest batch requests, with a materialize view.

> CREATE SOURCE webhook_for_batch_events IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT JSON

$ webhook-append name=webhook_for_batch_events
[
  { "event_id": 1 },
  { "event_id": 2 },
  { "event_id": 3 },
  { "event_id": 4 }
]

> CREATE MATERIALIZED VIEW webhook_for_batch_events_flattened (body)
  IN CLUSTER webhook_compute
  AS (
    SELECT jsonb_array_elements(body) as body FROM webhook_for_batch_events
  );

> SELECT body FROM webhook_for_batch_events_flattened
"{\"event_id\":1}"
"{\"event_id\":2}"
"{\"event_id\":3}"
"{\"event_id\":4}"

$ webhook-append name=webhook_for_batch_events
[
  { "event_id": 5 },
  { "event_id": 6 },
  { "event_id": 7 },
  { "event_id": 8 }
]

> SELECT COUNT(*) FROM webhook_for_batch_events_flattened
8

# Unnest batch requests with FORMAT JSON ARRAY.

> CREATE SOURCE webhook_json_array IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT JSON ARRAY

$ webhook-append name=webhook_json_array
[
  { "event_name": "a" },
  { "event_name": "b" },
  { "event_name": "c" },
  { "event_name": "a" }
]

> SELECT body FROM webhook_json_array
"{\"event_name\":\"a\"}"
"{\"event_name\":\"a\"}"
"{\"event_name\":\"b\"}"
"{\"event_name\":\"c\"}"

$ webhook-append name=webhook_json_array
{ "event_type": "i am a single event" }

> SELECT body FROM webhook_json_array
"{\"event_name\":\"a\"}"
"{\"event_name\":\"a\"}"
"{\"event_name\":\"b\"}"
"{\"event_name\":\"c\"}"
"{\"event_type\":\"i am a single event\"}"

$ webhook-append name=webhook_json_array
[ { "nested_array": "a_1" }, { "nested_array": "a_2" } ]
[ { "nested_array": "b_1" }, { "nested_array": "b_2" } ]

> SELECT body FROM webhook_json_array
"{\"event_name\":\"a\"}"
"{\"event_name\":\"a\"}"
"{\"event_name\":\"b\"}"
"{\"event_name\":\"c\"}"
"{\"event_type\":\"i am a single event\"}"
"{\"nested_array\":\"a_1\"}"
"{\"nested_array\":\"a_2\"}"
"{\"nested_array\":\"b_1\"}"
"{\"nested_array\":\"b_2\"}"

> CREATE SOURCE webhook_ndjson IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT JSON

$ webhook-append name=webhook_ndjson
{ "name": "bill" }
{ "name": "john" }
{ "name": "alex" }

> SELECT body FROM webhook_ndjson
"{\"name\":\"bill\"}"
"{\"name\":\"john\"}"
"{\"name\":\"alex\"}"

> CREATE SOURCE webhook_json_array_with_headers IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT JSON ARRAY
  INCLUDE HEADER 'x-timestamp' as event_timestamp
  CHECK (
    WITH ( HEADERS, BODY )
    decode(headers->'x-signature', 'base64') = hmac(body, 'super_secret_123', 'sha256')
  )

$ webhook-append name=webhook_json_array_with_headers x-timestamp=200 x-signature=a5nfOLZK7Xp5yuJeukC7A3XhNo5Y0gPQmg9KVDo9hU8=
[
  { "event": "coolio_thing" },
  { "event": "oof_not_good" },
  { "event": "recovered" }
]

> SELECT body, event_timestamp FROM webhook_json_array_with_headers
"{\"event\":\"coolio_thing\"}" 200
"{\"event\":\"oof_not_good\"}" 200
"{\"event\":\"recovered\"}" 200

$ webhook-append name=webhook_json_array_with_headers x-timestamp=202 x-signature=aUyjzZBzSvGGZVwMncpZ8mZABmgr2L13quRZy6uaTgA=
{ "event": "ugh_wish_i_was_batch" }

> SELECT body FROM webhook_json_array_with_headers WHERE event_timestamp = '202'
"{\"event\":\"ugh_wish_i_was_batch\"}"

# Renaming a webhook source.

> CREATE SOURCE webhook_foo IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT

$ webhook-append name=webhook_foo
aaa_1

> SELECT body FROM webhook_foo;
aaa_1

> ALTER SOURCE webhook_foo RENAME TO webhook_bar

$ webhook-append name=webhook_foo status=404
aaa_2

$ webhook-append name=webhook_bar
bbb_1

> SELECT body FROM webhook_bar;
aaa_1
bbb_1

# Renaming the schema that contains a webhook source.

> CREATE SCHEMA my_webhooks;

> CREATE SOURCE my_webhooks.foo IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT

> CREATE SOURCE my_webhooks.bar IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT

$ webhook-append schema=my_webhooks name=foo
foo-foo

$ webhook-append schema=my_webhooks name=bar
bar-bar

> SELECT body FROM my_webhooks.foo
foo-foo

> SELECT body FROM my_webhooks.bar
bar-bar

> BEGIN;

> ALTER SCHEMA my_webhooks RENAME TO other_webhooks;

> ALTER SOURCE other_webhooks.bar RENAME TO baz;

> COMMIT;

$ webhook-append schema=other_webhooks name=foo
foo-after

$ webhook-append schema=other_webhooks name=baz
baz-after

> SELECT body FROM other_webhooks.foo
foo-foo
foo-after

> SELECT body FROM other_webhooks.baz
bar-bar
baz-after

# Dropping a webhook source should drop the underlying persist shards.

$ set-from-sql var=webhook-source-id
SELECT id FROM mz_sources WHERE name = 'webhook_bytes';

$ set-from-sql var=webhook-source-shard-id
SELECT shard_id FROM mz_internal.mz_storage_shards WHERE object_id = '${webhook-source-id}';

> SELECT COUNT(*) FROM mz_internal.mz_storage_shards WHERE object_id = '${webhook-source-id}';
1

> DROP SOURCE webhook_bytes CASCADE;

> SELECT COUNT(*) FROM mz_internal.mz_storage_shards WHERE object_id = '${webhook-source-id}';
0

$ check-shard-tombstone shard-id=${webhook-source-shard-id}

# Cleanup.
> DROP CLUSTER webhook_cluster CASCADE;

# Creating a webhook source without specifying the cluster.
$ skip-if
SELECT ${arg.replicas} > 1;

$ set-from-sql var=current-cluster
SHOW cluster;

> CREATE SOURCE webhook_in_current FROM WEBHOOK BODY FORMAT JSON;

> SELECT c.name = '${current-cluster}' FROM mz_clusters c JOIN mz_sources s ON c.id = s.cluster_id WHERE s.name = 'webhook_in_current';
true
