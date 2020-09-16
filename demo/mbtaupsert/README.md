# Real time application demo using the MBTA API

## What is the MBTA API?

Massachusetts Bay Transportation Authority manages public transit in the Boston
area. It has a bunch of live JSON server-sent event source streams whose format
are roughly like this:

```
event:reset
[{id: ..., type: ..., other_fields: ...},
 {id: ..., type: ..., other_fields: ...},
 {id: ..., type: ..., other_fields: ...}, ...]

event: update
{id: ..., type: ..., other_fields: ...}

event: remove
{id: ..., type: ...}
```

## What will I be able to do in this demo?

You will be able to issue SQL queries and maintain views on the live state of
the Boston-area public transit system.
[doc/mbta-predictions.md](/doc/mbta-predictions.md) contains some inspiration to
get you started. It tells you:
* How to reproduce the MBTA countdown clock.
* How to find the quickest way from stop A to stop B.

In addition, you can run SQL queries and maintain views on the history of the
state of the Boston-area public transit system from the time you started running
the demo onwards by omitting the `ENVELOPE UPSERT` when creating a source.

## What does this code do?

This code is a workaround for the fact that we don't yet support:
* directly connecting server-sent event sources yet.
  (Follow progress on this at #2237)
* converting a kafka topic not in key-value format to key-value format.
  (Follow progress on this at #1576)
It takes a file where the MBTA live stream is being written to and converts the
data into a Kafka stream of the key-value format:
* ```
   event: remove
   {id: some_id, type: ...}
   ```
   gets converted to a Kafka message with key `some_id` and null payload
* ```
    event: update
    {id: some_id, type: ..., other_field1: ..., other_field2: ..., etc}
    ```
    gets converted to a Kafka message with key `some_id` and payload
    ```
    {other_field1: ..., other_field2: ..., etc}
    ```
* ```
   event:reset
   [{id: ..., type: ..., other_field1: ..., other_field2: ..., etc},
    {id: ..., type: ..., other_field1: ..., other_field2: ..., etc},
    {id: ..., type: ..., other_field1: ..., other_field2: ..., etc}, ...]
   ```
   gets converted into one Kafka per message per object in the array. Each
   message has key `whatever_the_id_was` and payload
   ```
   {other_field1: whatever_is_in_this_field, other_field2: ..., etc}
   ```

Technically, this code is not MBTA stream-specific. With a few lines of changes,
it should be able to take any stream of json objects, parse out the desired key,
and then produce a key-value Kafka topic out of it.

Look in [doc/mbtaupsert-doc.md](doc/mbtaupsert-doc.md) for more information.
