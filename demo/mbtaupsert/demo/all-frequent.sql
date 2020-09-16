DROP SOURCE IF EXISTS mbta_directions CASCADE;
DROP SOURCE IF EXISTS mbta_stops CASCADE;
DROP SOURCE IF EXISTS mbta_routes CASCADE;
DROP SOURCE IF EXISTS all_pred CASCADE;
DROP SOURCE IF EXISTS all_trips CASCADE;
DROP SOURCE IF EXISTS all_schd CASCADE;
DROP SOURCE IF EXISTS vehicles CASCADE;

CREATE MATERIALIZED SOURCE mbta_directions
  FROM FILE '/workdir/workspace/MBTA_GTFS/directions.txt' FORMAT CSV WITH HEADER;

CREATE MATERIALIZED SOURCE mbta_stops
  FROM FILE '/workdir/workspace/MBTA_GTFS/stops.txt' FORMAT CSV WITH HEADER;

CREATE MATERIALIZED SOURCE mbta_routes
  FROM FILE '/workdir/workspace/MBTA_GTFS/routes.txt' FORMAT CSV WITH HEADER;

CREATE SOURCE all_pred
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'all-pred'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;

CREATE SOURCE all_trip
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'all-trip'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;

CREATE SOURCE all_schd
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'all-schd'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;

CREATE SOURCE vehicles
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'all-vehicles'
  FORMAT TEXT ENVELOPE UPSERT FORMAT TEXT;

CREATE MATERIALIZED VIEW parsed_all_pred as
SELECT id,
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
FROM (SELECT key0 as id, cast (text as jsonb) AS payload FROM all_pred);

CREATE MATERIALIZED VIEW parsed_all_schd as
SELECT id,
CAST(payload->'attributes'->>'arrival_time' AS timestamptz) arrival_time,
CAST(payload->'attributes'->>'departure_time'  AS timestamptz) departure_time,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
CAST(CAST(payload->'attributes'->>'stop_sequence' AS DECIMAL(5,1)) AS INT) stop_sequence,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id
FROM (SELECT key0 as id, cast ("text" as jsonb) AS payload FROM all_schd);

CREATE MATERIALIZED VIEW parsed_all_trip as
SELECT id,
payload->'attributes'->>'bikes_allowed' bikes_allowed,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'attributes'->>'headsign' headsign,
payload->'attributes'->>'wheelchair_accessible' wheelchair_accessible,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'route_pattern'->'data'->>'id' route_pattern_id,
payload->'relationships'->'service'->'data'->>'id' service_id,
payload->'relationships'->'shape'->'data'->>'id' shape_id
FROM (SELECT key0 as id, cast ("text" as jsonb) AS payload FROM all_trip);

CREATE MATERIALIZED VIEW parsed_vehicles as
SELECT id,
payload->'attributes'->>'current_status' status,
CAST(CAST(payload->'attributes'->>'direction_id' AS DECIMAL(5,1)) AS INT) direction_id,
payload->'relationships'->'route'->'data'->>'id' route_id,
payload->'relationships'->'stop'->'data'->>'id' stop_id,
payload->'relationships'->'trip'->'data'->>'id' trip_id
FROM (SELECT key0 as id, cast ("text" as jsonb) AS payload FROM vehicles);
