---
headless: true
---

The monitoring stack installs Alertmanager and a default set of alert rules for
Materialize and the Kubernetes platform under it. A default install configures
no receiver, so no one is notified until you configure one. To route alerts to
PagerDuty, Slack, email, or another receiver, and to tune which rules install,
see [Alerting](/observability/self-managed/alerting/).

If you alert from a platform you already run instead, see [Thresholds for alerts
you build yourself](/observability/self-managed/alerting/#thresholds).
