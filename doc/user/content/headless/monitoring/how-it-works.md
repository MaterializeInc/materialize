---
headless: true
---

Materialize captures and stores logs and metrics.

```mermaid
flowchart LR
    subgraph src["Sources in your cluster"]
        MZ["Materialize pods<br/>(environmentd, clusterd, operator)"]
        INFRA["Cluster telemetry<br/>(kube-state-metrics, node exporter, cAdvisor)"]
        LOGS[("Container logs<br/>and node journals")]
        EV["Kubernetes events"]
    end

    AGENT["Alloy agent<br/>(DaemonSet, one per node)"]
    GW["Alloy gateway<br/>(scrape, normalize, enrich, fan out)"]

    subgraph bundled["Bundled stores, in your cluster"]
        THANOS[("Thanos<br/>metrics")]
        LOKI[("Loki<br/>logs")]
    end

    OBJ[("Your object storage")]
    GRAF["Grafana<br/>(dashboards and alerts)"]
    EXT["Platforms you already run<br/>(Datadog, Honeycomb, any OTLP<br/>endpoint, Prometheus remote write)"]

    LOGS --> AGENT
    AGENT -- "logs" --> GW
    MZ -- "metrics, via ServiceMonitor and PodMonitor" --> GW
    INFRA -- "metrics" --> GW
    EV -- "as logs" --> GW
    GW -- "metrics" --> THANOS
    GW -- "logs" --> LOKI
    GW -- "metrics and logs" --> EXT
    THANOS --> OBJ
    LOKI --> OBJ
    THANOS -- "PromQL" --> GRAF
    LOKI -- "LogQL" --> GRAF
```

1. A **Grafana Alloy agent** runs as a DaemonSet on every node and tails
   container logs.

1. A **Grafana Alloy gateway** does the processing. It receives logs from the
   agents, collects Kubernetes events, and scrapes metrics from Materialize and
   from the cluster using the `ServiceMonitor` and `PodMonitor` resources the
   chart installs. It normalizes and enriches both streams.

1. The gateway **forwards** each stream to one or more destinations. Metrics go
   to any number of metric backends, and logs to any number of log backends.

The default install points the gateway at storage that runs inside your cluster:
**Thanos** for metrics and **Loki** for logs, both persisting to object storage
the Terraform modules create. Neither is something you interact with directly.
You query them through Grafana, and you can add or replace them without changing
how anything is collected.

Because the fan-out happens at the gateway, every destination is configured in
one place, and each one receives its own independently filtered copy.
