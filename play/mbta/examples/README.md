# Example files

This directory contains config files for spinning up a bunch of MBTA streams and
converting them into Kafka topics en masse so you can play around with the data
without looking up all the routes.

For each config file, there is a corresponding sql file of sample sources/views
that you can create on the generated topics. The SQL assumes that you are
using the automatic Docker setup; if that is not the case, change the source paths.

## Large-scale experimentation

* [all-frequent-routes-config-weekend.csv](all-frequent-routes-config-weekend.csv)
  contains
  * the vehicles stream
  * all predictions, schedules, and trips streams for all rapid transit lines
    and key bus routes. In other words, it contains everything in the standard
    [MBTA Rapid Transit/Key Bus Routes Map](https://www.mbta.com/schedules/subway).
* [all-frequent-routes-config-weekday.csv](all-frequent-routes-config-weekday.csv)
  contains all streams contained in `all-frequent-routes-config-weekend`, plus
  predictions, schedules, and trips streams that correspond to crosstown buses,
  which only run on weekdays.
