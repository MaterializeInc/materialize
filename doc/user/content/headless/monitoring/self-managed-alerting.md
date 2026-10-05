---
headless: true
---

The monitoring stack installs Alertmanager and a default set of alert rules for
Materialize and the Kubernetes platform under it. A default install configures
no receiver, so no one is notified until you configure one. To route alerts to
PagerDuty, Slack, Microsoft Teams, email, or a webhook, see
[Alerting](/observability/self-managed/alerting/). To route, tune, and silence
them further, see [Customize
alerting](/observability/self-managed/customize-alerting/).

If you alert from a platform you already run instead, see [Thresholds for alerts
you build yourself](/observability/self-managed/alerting/#thresholds).
