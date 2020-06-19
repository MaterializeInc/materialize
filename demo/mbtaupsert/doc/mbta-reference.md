# View Creation Reference for MBTA Datasets

The following are templates to help you get started on exploring the MBTA data
within Materialize.

## File sources

Any file in `workspace` will be mounted to the Materialize docker container at
`/workdir/workspace`.

### Static metadata

```
CREATE SOURCE mbta_x_metadata
  FROM FILE '/path-to-file' FORMAT CSV WITH HEADER;
```

### Current time stream

If you have done automatic setup, there is a file `workspace/current_time` that
where the current epoch time is printed every second. You can use the file to
create a view whose only record is always the current time.

The option `tail=true` will ensure that Materialize will keep checking the file
for updates.

```
CREATE SOURCE current_time 
FROM FILE 'path-to-workspace-directory/workspace/current_time' WITH(tail=true) FORMAT TEXT

CREATE MATERIALIZED VIEW current_time_v AS
SELECT max(to_timestamp(cast(text as int))) AS now 
FROM current_time;
```

## Live streams

First, create a source from the kafka topic.

```
CREATE SOURCE x
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'x-live'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;
```

For some of the streams, such as predictions or trips, MBTA requires that
you use a filter in the API, so the data you want may be separated into multiple
streams. The best way to handle this is using the [configuration
file](../../demo/mbta-setup.md#automatic-stream-setup) to write the streams into
the same kafka topic.

If you want really want one Kafka topic per MBTA stream, you may want create a
view to union them together. For example, if you want to have predictions for
the entirety of the Green Line, you need to combine the streams for Green-B, Green-C, Green-D, and Green-E like this: 
```
CREATE VIEW all-green-pred as
  SELECT * FROM green-b-pred UNION
  SELECT * FROM green-c-pred UNION
  SELECT * FROM green-d-pred UNION
  SELECT * FROM green-e-pred;
```

Then, you build a view on top of your source or view combining multiple sources
that parses the JSON data that has come in from the stream. For your
convenience, sample JSON parsing view creation queries are listed below for some
of the live streams. In general, you should only parse out the fields that are
of interest to you.

### Predictions

```
CREATE VIEW parsed_x_pred as 
SELECT pred_id,
CAST(payload->'attributes'->>'arrival_time' AS timestamptz) arrival_time,
CAST(payload->'attributes'->>'departure_time'  AS timestamptz) departure_time,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'attributes'->>'schedule_relationship' schedule_relationship,
payload->'attributes'->>'status' status,
CAST(CAST(payload->'attributes'->>'stop_sequence' AS DECIMAL(5,1)) AS INT) stop_sequence,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id,
payload->'relationships'->'vehicle'->'data'->>'id' vehicle_id
FROM (SELECT key0 as pred_id, cast ("text" as jsonb) AS payload FROM x_pred);
```

### Schedules

```
CREATE VIEW parsed_x_schd as 
SELECT schd_id,
CAST(payload->'attributes'->>'arrival_time' AS timestamptz) arrival_time,
CAST(payload->'attributes'->>'departure_time'  AS timestamptz) departure_time,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
CAST(CAST(payload->'attributes'->>'stop_sequence' AS DECIMAL(5,1)) AS INT) stop_sequence,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id
FROM (SELECT key0 as schd_id, cast ("text" as jsonb) AS payload FROM x_schd);
```

### Trips

```
CREATE VIEW parsed_x_trips as 
SELECT trip_id,
payload->'attributes'->>'bikes_allowed' bikes_allowed,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'attributes'->>'headsign' headsign,
payload->'attributes'->>'wheelchair_accessible' wheelchair_accessible,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'route_pattern'->'data'->>'id' route_pattern_id,
payload->'relationships'->'service'->'data'->>'id' service_id,
payload->'relationships'->'shape'->'data'->>'id' shape_id
FROM (SELECT key0 as trip_id, cast ("text" as jsonb) AS payload FROM x_trips);
```

### Vehicles

```
CREATE VIEW parsed_vehicles as 
SELECT vehicle_id,
payload->'attributes'->>'current_status' status,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id
FROM (SELECT key0 as vehicle_id, cast ("text" as jsonb) AS payload FROM vehicles);
```
