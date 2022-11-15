# Local Prometheus testing

### Usage

```
docker run -p 9090:9090 -v $(pwd)/misc/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
```

Note: on linux you will additionally need `--add-host host.docker.internal:host-gateway`!

You can adjust `prometheus.yml` to add specific `storaged` and `computed` instances.


Go to <http://localhost:9090/graph> for the Prometheus UI.
