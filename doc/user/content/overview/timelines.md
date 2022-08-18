---
title: "Timelines"
description: "Timelines describe sources' meaning of time."
weight: 20
aliases:
  - /sql/timelines/
draft: true
#menu:
  #main:
    #parent: advanced
    #weight: 60
---

All data in Materialize has a timestamp.
Most sources, and all tables, created by Materialize use the system time (milliseconds since the Unix epoch) when the data were ingested as their timestamp.
[CDC sources][cdc-sources] require users to specify the timestamps of their data, but the timestamps might not match the timestamp format of the system time.
For example, they could use seconds instead of milliseconds, or just be a counter that starts at 1 and increments.

We use the term **timeline** to describe the meaning of this timestamp number.
All sources are assigned to a timeline.
Materialize prevents joining sources from different timelines because it cannot guarantee that a meaningful result will ever be returned.
Users can specify custom timelines if they need to join sources that are by default on different timelines.

## Default Timeline

- [CDC sources][cdc-sources] default to their own individual timeline, and cannot be joined with any other source (even other CDC sources).
- All other sources (and all tables) use the system timeline.

## User Timelines

When creating a source, users can specify a `timeline` as a `WITH` option.
This can be used to allow multiple CDC sources, or a CDC source and system time sources, to be joinable.

For example, to create two CDC sources that are joinable:

```sql
CREATE SOURCE source_1
  FROM KAFKA BROKER 'broker' TOPIC 'topic-1'
    WITH (timeline='user')
  FORMAT AVRO USING SCHEMA 'schema-1'
  ENVELOPE MATERIALIZE;

CREATE SOURCE source_2
  FROM KAFKA BROKER 'broker' TOPIC 'topic-2'
    WITH (timeline='user')
  FORMAT AVRO USING SCHEMA 'schema-2'
  ENVELOPE MATERIALIZE;
```

## CDC Sources

[CDC sources][cdc-sources] supports a `epoch_ms_timeline` `WITH` option that moves it to the system timeline, making the CDC source joinable to tables and other system timeline sources.
Users **must** ensure that the `time` field's units are milliseconds since the Unix epoch.
Joining this source to other system time sources will result in query delays until the timestamps being received are close to wall-clock `now()`.

```sql
CREATE SOURCE source_3
  FROM KAFKA BROKER 'broker' TOPIC 'topic-3'
    WITH (epoch_ms_timeline=true)
  FORMAT AVRO USING SCHEMA 'schema'
  ENVELOPE MATERIALIZE
```

[cdc-sources]: /connect/materialize-cdc
