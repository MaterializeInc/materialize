Example Prometheus config
=========================

Materialized exposes prometheus metrics on its primary port, via http, at the default
prometheus `/metrics` path.

If you have prometheus installed (`brew install prometheus` on a mac) then just running
`prometheus` inside this directory will expose a dashboard at `localhost:9090`.

If you have the chbench example running in the background you'll need to use a different
port for prometheus, e.g. `prometheus --web.listen-address="127.0.0.1:9091"`

materialize metrics are all exposed with a `mz_*` prefix, so e.g.
`rate(mz_responses_sent_total[10s])` will show you the number of responses averaged over
10 second windows.
