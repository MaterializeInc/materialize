A `materialized` Grafana dashboard
==================================

This directory contains the implementation of a "dashboard in a box", a Docker image that
contains both Prometheus and Grafana (and some other tools) so that folks can very
quickly introspect their materialized, before they're ready to necessarily spin up
production monitoring pointing at materialized.

User documentation is at https://materialize.io/docs/monitoring/ (or
[doc/user/content/monitoring/_index.md][doc] locally).


## Updating the dashboard

The source of truth for this dashboard is http://grafana.mz/d/materialize-overview ,
which we keep up to date as we run load tests and discover better ways to monitor
ourselves.

If you want to modify the dashboard that we ship to users, you should instead modify the
"Materialized Overview" dashboard in grafana.mz, and save it, keeping it canonical for
ourselves.

Before releases, or whenever we'd like to update the dashboard we ship to users, we sync
the dashboard from grafana.mz to this directory, using the following procedure:

* downloading the json model from http://grafana.mz/d/materialize-overview/materialize-overview-load-tests?editview=dashboard_json
* running the `bin/dashboard-clean` script on that downloaded json
* ensuring that `conf/grafana/dashboards/overview.json` has the new model json
* building the dashboard locally `mzimage build dashboard`
* running the dashboard inside a demo: `mzconduct run billing -w load-test`
* editing the dashboard to remove some _more_ of our cruft
* copying the json model again
* running `bin/dashboard-clean` again

... that's all.


[doc]: ../../../doc/user/content/monitoring/_index.md
