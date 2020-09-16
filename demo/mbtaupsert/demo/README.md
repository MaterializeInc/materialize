# Files related to demos

This directory contains config files for spinning up a bunch of MBTA streams and
converting them into Kafka topics en masse.

For each config file, there is a corresponding sql file of sample sources/views
that you can create on the generated topics. The SQL assumes that you are
running the demo using the automatic Docker setup; if that is not the case,
change the source paths.

## Large-scale experimentation

* [all-frequent-routes-config-weekend](all-frequent-routes-config-weekend)
  contains
  * the vehicles stream
  * all predictions, schedules, and trips streams for all rapid transit lines
    and key bus routes. In other words, it contains everything in the standard
    [MBTA Rapid Transit/Key Bus Routes Map](https://www.mbta.com/schedules/subway).
* [all-frequent-routes-config-weekday](all-frequent-routes-config-weekday)
  contains all streams contained in `all-frequent-routes-config-weekend`, plus
  predictions, schedules, and trips streams that correspond to crosstown buses,
  which only run on weekdays.

## Create your config file

If you are running the demo with docker, the config file should go in this
directory. The docker container will mount this directory at `/workdir/config`.

The config file should be in CSV format with no spaces in between. For each MBTA stream,
create a row with the following six fields in order:
1. Name of kafka topic to ingest into. Optional. Defaults to
   `mbta-$api-$filter_type-$filter_id` if there is a filter and `mbta-$api` if
   there is not. You can assign multiple MBTA streams to go into the same Kafka
   topic. It is recommended to assign multiple MBTA streams to the same Kafka
   topic when they come from the same API.

1. The top level API of the stream.

1. Type of id to filter on. Optional. If this is specified, the field
   "Id to filter on" should also be specified.

1. Id to filter on.

1. Number of partitions that the kafka topic should have. Optional. Defaults to 1.

1. Heartbeat frequency. In other words, the frequency at which the `mbtaupsert`
   code should check the file for new data if it has caught up to the end of the
   file. Optional. Defaults to 250ms. Specifying this is mostly just for
   avoiding unnecessary usage of CPU cycles for slow-moving topics.

Here's a mapping of example apis you want to create a stream for, together with
how it should be specified in the config file:
* `/trips/?filter[route]=Green-E` to a 5-partition Kafka topic, and only check
  it every 30 seconds because this is an infrequently updated stream:
  `,trips,route,Green-E,5,30s`
* `/trips/?alerts` to a Kafka topic named `alerts-live`: `alerts-live,alerts,,,,`

List of the live streams is here:
https://api-v3.mbta.com/docs/swagger/index.html
