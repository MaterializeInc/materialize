---
headless: true
---

The `COMMIT INTERVAL` setting controls how frequently Materialize commits
snapshots to your Iceberg table, making the data available to downstream query
engines. This setting involves tradeoffs:

| Shorter intervals (e.g., < `1m`) | Longer intervals (e.g., `5m`) |
|---------------------------------|-------------------------------|
| Lower latency - data visible sooner in downstream systems | Higher latency - data takes longer to appear |
| More small files - can degrade query performance over time | Fewer, larger files - better query performance |
| More frequent snapshot commits - higher catalog overhead | Less catalog overhead |
| Lower throughput efficiency | Higher throughput efficiency |

**Recommendations:**
- For production, use intervals of `1m` or longer
- For batch analytics, use longer intervals (`5m` to `15m`)

Starting in v26.34, you can change the commit interval of an existing sink with
[`ALTER SINK`](/sql/alter-sink/).

{{< note >}}
Outside of development environments, commit intervals should be at least `1m`.
Short commit intervals increase catalog overhead and produce many small files.
Small files will result in degraded query performance. It also increases load on
the Iceberg metadata, which can result in a degraded catalog, and non-responsive
queries.
{{< /note >}}
