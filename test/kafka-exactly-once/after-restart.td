# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema=[
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "update",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "data",
          "type": {
            "type": "record",
            "name": "data",
            "fields": [
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          }
        },
        {
          "name": "time",
          "type": "long"
        },
        {
          "name": "diff",
          "type": "long"
        }
      ]
    }
  },
  {
    "type": "record",
    "name": "progress",
    "namespace": "com.materialize.cdc",
    "fields": [
      {
        "name": "lower",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "upper",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "counts",
        "type": {
          "type": "array",
          "items": {
            "type": "record",
            "name": "counts",
            "fields": [
              {
                "name": "time",
                "type": "long"
              },
              {
                "name": "count",
                "type": "long"
              }
            ]
          }
        }
      }
    ]
  }
  ]

$ kafka-ingest format=avro topic=input schema=${schema}
{"array":[{"data":{"a":4,"b":1},"time":4,"diff":1}]}
{"array":[{"data":{"a":5,"b":2},"time":4,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[4],"upper":[5],"counts":[{"time":4,"count":2}]}}

$ kafka-verify headers=mz-timestamp format=avro sink=materialize.public.output sort-messages=true
1	{"before": null, "after": {"row": {"a": 1, "b": 1}}}
1	{"before": null, "after": {"row": {"a": 1, "b": 2}}}
1	{"before": null, "after": {"row": {"a": 2, "b": 1}}}
1	{"before": null, "after": {"row": {"a": 3, "b": 1}}}

$ kafka-verify headers=mz-timestamp format=avro sink=materialize.public.output sort-messages=true
2	{"before": null, "after": {"row": {"a": 11, "b": 11}}}
2	{"before": null, "after": {"row": {"a": 22, "b": 11}}}

$ kafka-verify headers=mz-timestamp format=avro sink=materialize.public.output sort-messages=true
3	{"before": null, "after": {"row": {"a": 3, "b": 4}}}
3	{"before": null, "after": {"row": {"a": 5, "b": 6}}}

$ kafka-verify headers=mz-timestamp format=avro sink=materialize.public.output sort-messages=true
4	{"before": null, "after": {"row": {"a": 4, "b": 1}}}
4	{"before": null, "after": {"row": {"a": 5, "b": 2}}}
