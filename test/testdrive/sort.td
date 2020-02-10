# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "type": "record",
            "name": "row",
            "fields": [
              { "name": "quote", "type": "string" },
              { "name": "val", "type": "long" }
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

> CREATE SOURCE foobar FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-foobar-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

$ kafka-ingest format=avro topic=foobar schema=${schema} timestamp=42
{"before": null, "after": {"quote": "I have a theory that it's impossible to prove anything, but I can't prove it.", "val": 2079}}
{"before": null, "after": {"quote": "It was a virgin forest, a place where the Hand of Man had never set foot.", "val": 12345}}
{"before": null, "after": {"quote": "If it pours before seven, it has rained by eleven.", "val": 12345}}
{"before": null, "after": {"quote": "All power corrupts, but we need electricity.", "val": 12345}}
{"before": null, "after": {"quote": "I want to read my new poem about pork brains and outer space ...", "val": 6745}}
{"before": null, "after": {"quote": "You are magnetic in your bearing.", "val": 24223}}
{"before": null, "after": {"quote": "Yes, but every time I try to see things your way, I get a headache.", "val": 21243}}
{"before": null, "after": {"quote": "Ring around the collar.", "val": 1735}}
{"before": null, "after": {"quote": "New York is real.  The rest is done with mirrors.", "val": 25040}}

$ kafka-ingest format=avro topic=foobar schema=${schema} timestamp=43
{"before": null, "after": null}

> CREATE MATERIALIZED VIEW foobar_view as SELECT * from foobar

> SELECT val, quote FROM foobar_view ORDER BY quote LIMIT 5
val    quote
--------------------------------------------------------------------------------------
12345  "All power corrupts, but we need electricity."
2079   "I have a theory that it's impossible to prove anything, but I can't prove it."
6745   "I want to read my new poem about pork brains and outer space ..."
12345  "If it pours before seven, it has rained by eleven."
12345  "It was a virgin forest, a place where the Hand of Man had never set foot."

> SELECT val, quote FROM foobar_view ORDER BY quote ASC LIMIT 5
val    quote
--------------------------------------------------------------------------------------
12345  "All power corrupts, but we need electricity."
2079   "I have a theory that it's impossible to prove anything, but I can't prove it."
6745   "I want to read my new poem about pork brains and outer space ..."
12345  "If it pours before seven, it has rained by eleven."
12345  "It was a virgin forest, a place where the Hand of Man had never set foot."

> SELECT * FROM foobar_view ORDER BY quote DESC LIMIT 5
quote                                                                        val
----------------------------------------------------------------------------------
"You are magnetic in your bearing."                                          24223
"Yes, but every time I try to see things your way, I get a headache."        21243
"Ring around the collar."                                                    1735
"New York is real.  The rest is done with mirrors."                          25040
"It was a virgin forest, a place where the Hand of Man had never set foot."  12345

# Test that the second-column sort works fine
> SELECT * FROM foobar_view ORDER BY val, quote LIMIT 6
quote val
---------
"Ring around the collar."                                                         1735
"I have a theory that it's impossible to prove anything, but I can't prove it."   2079
"I want to read my new poem about pork brains and outer space ..."                6745
"All power corrupts, but we need electricity."                                    12345
"If it pours before seven, it has rained by eleven."                              12345
"It was a virgin forest, a place where the Hand of Man had never set foot."       12345

> SELECT * FROM foobar_view ORDER BY val, quote DESC LIMIT 6
quote                                                                             val
---------------------------------------------------------------------------------------
"Ring around the collar."                                                         1735
"I have a theory that it's impossible to prove anything, but I can't prove it."   2079
"I want to read my new poem about pork brains and outer space ..."                6745
"It was a virgin forest, a place where the Hand of Man had never set foot."       12345
"If it pours before seven, it has rained by eleven."                              12345
"All power corrupts, but we need electricity."                                    12345

> SELECT val, quote FROM foobar_view ORDER BY quote OFFSET 5 ROWS
val    quote
----------------------------------------------------------------------------
25040  "New York is real.  The rest is done with mirrors."
1735   "Ring around the collar."
21243  "Yes, but every time I try to see things your way, I get a headache."
24223  "You are magnetic in your bearing."

> SELECT val, quote FROM foobar_view ORDER BY quote LIMIT 4 OFFSET 0 ROWS
val    quote
--------------------------------------------------------------------------------------
12345  "All power corrupts, but we need electricity."
2079   "I have a theory that it's impossible to prove anything, but I can't prove it."
6745   "I want to read my new poem about pork brains and outer space ..."
12345  "If it pours before seven, it has rained by eleven."

> SELECT val, quote FROM foobar_view ORDER BY val, quote LIMIT 3 OFFSET 2 ROWS
val    quote
--------------------------------------------------------------------------------------
6745   "I want to read my new poem about pork brains and outer space ..."
12345  "All power corrupts, but we need electricity."
12345  "If it pours before seven, it has rained by eleven."

> SELECT val, quote FROM foobar_view ORDER BY quote DESC LIMIT 5 OFFSET 6 ROWS
val    quote
--------------------------------------------------------------------------------------
6745   "I want to read my new poem about pork brains and outer space ..."
2079   "I have a theory that it's impossible to prove anything, but I can't prove it."
12345  "All power corrupts, but we need electricity."

> SELECT val, quote FROM foobar_view ORDER BY quote DESC LIMIT 4 OFFSET 10 ROWS
val    quote
--------------------------------------------------------------------------------------

> SELECT val, quote FROM foobar_view ORDER BY quote DESC OFFSET 10 ROWS
val    quote
--------------------------------------------------------------------------------------

> SELECT val, quote FROM foobar_view ORDER BY quote DESC OFFSET 0 ROWS
val    quote
--------------------------------------------------------------------------------------
24223  "You are magnetic in your bearing."
21243  "Yes, but every time I try to see things your way, I get a headache."
1735   "Ring around the collar."
25040  "New York is real.  The rest is done with mirrors."
12345  "It was a virgin forest, a place where the Hand of Man had never set foot."
12345  "If it pours before seven, it has rained by eleven."
6745   "I want to read my new poem about pork brains and outer space ..."
2079   "I have a theory that it's impossible to prove anything, but I can't prove it."
12345  "All power corrupts, but we need electricity."
