---
title: "Alerting"
description: "How to send the alerts the Self-Managed Materialize monitoring stack raises to PagerDuty, Slack, Microsoft Teams, email, or a webhook, and which thresholds to alert on in another platform."
menu:
  main:
    parent: "monitor-sm"
    name: "Set alerts"
    weight: 15
    identifier: "alerting-sm"
aliases:
  - /manage/monitor/self-managed/alerting/
---

The monitoring stack that the [Materialize Terraform
modules](/self-managed-deployments/installation/#install-using-terraform-modules)
install evaluates a set of bundled alert rules and delivers what fires through
**Alertmanager**. This guide walks you through sending those alerts to a
receiver, such as PagerDuty, Slack, Microsoft Teams, email, or a webhook.

{{< warning >}}
A default install configures no receiver. Until you configure one, every alert
is routed to `mzmon-null`, which notifies nobody.
{{< /warning >}}

If you alert from a platform you already run instead, such as Datadog or
Honeycomb, see [Thresholds for alerts you build yourself](#thresholds) for the
metrics and thresholds to start from.

## How it works

The stack evaluates a default set of alert rules covering Materialize and the
Kubernetes platform under it, and sends whatever fires to Alertmanager, which
routes each alert to the receivers that serve its severity. For every bundled
rule, see [Common Alerts
⧉](https://materializeinc.github.io/materialize-monitoring/reference/common-alerts/),
and for how routing works, see [Customize
alerting](/observability/self-managed/customize-alerting/#how-routing-works).

## Instructions

### Before you begin

Ensure you have:

- A Materialize deployment created with the [Materialize Terraform
  modules](/self-managed-deployments/), with the monitoring stack enabled. See
  [Step 1](#step-1-enable-observability).

- [Terraform ⧉](https://developer.hashicorp.com/terraform/install) installed.

- [kubectl ⧉](https://kubernetes.io/docs/tasks/tools/) installed and configured
  to connect to your cluster.

- The credential for the receiver you plan to use, such as a PagerDuty routing
  key or a Slack incoming webhook URL.

{{< note >}}
The Terraform steps on this page require **v15.0.0** or later of the
Materialize Terraform Modules, which is where the `monitoring` module accepts
the alerting inputs. Upgrading to v15.0.0 does not change alerting on its own:
every alerting input defaults to empty, so a deployment that sets none of them
renders exactly as before.

If you install the `materialize-monitoring` chart with Helm rather than through
the Terraform modules, skip Step 1 and use the **Helm** tab in Step 2.
{{< /note >}}

### Step 1. Enable observability

{{% include-headless "/headless/monitoring/enable-observability" %}}

### Step 2. Configure a receiver

A receiver is an Alertmanager notification integration, plus the classes of
alert it serves:

| Receiver attribute | Purpose |
|--------------------|---------|
| `class` | The classes this receiver serves, as a string or a list. Under the default `important` preset, a receiver that should get every alert serves `high`, `normal`, and `low`. |
| `config` | An Alertmanager receiver, such as `slack_configs` or `pagerduty_configs`. Field names are Alertmanager's own. See the [receiver integration reference ⧉](https://prometheus.io/docs/alerting/latest/configuration/#receiver-integration-settings). |
| `route` | Optional. Route options applied wherever alerts are routed to this receiver, such as `group_wait` and `repeat_interval`. |

A receiver never holds its credential directly. It reads the credential from a
file on the `alertmanager-receivers` Secret, through the `_file` variant of the
field, such as `api_url_file` for Slack. Each key of that Secret is a file under
`/etc/alertmanager/secrets/alertmanager-receivers/`.

{{< tabs >}}
{{< tab "Terraform" >}}

In the `monitoring` module block of your Terraform, add a receiver under
`alerting`, and the credential it reads under `alerting_receiver_secrets`. The
examples ship an `alerting` block commented out in the `monitoring` module, so
you can uncomment it in place.

{{< tabs >}}
{{< tab "Slack" >}}

```hcl
module "monitoring" {
  # ...

  alerting = {
    receivers = {
      chat = {
        class = ["high", "normal", "low"]
        config = {
          slack_configs = [{
            channel       = "#materialize-alerts"
            api_url_file  = "/etc/alertmanager/secrets/alertmanager-receivers/slack-url"
            send_resolved = true
          }]
        }
      }
    }
  }

  alerting_receiver_secrets = {
    "slack-url" = var.slack_webhook_url
  }
}
```

{{< /tab >}}
{{< tab "PagerDuty" >}}

```hcl
module "monitoring" {
  # ...

  alerting = {
    receivers = {
      oncall = {
        class = ["high", "normal", "low"]
        config = {
          pagerduty_configs = [{
            routing_key_file = "/etc/alertmanager/secrets/alertmanager-receivers/pagerduty-key"
            send_resolved    = true
          }]
        }
      }
    }
  }

  alerting_receiver_secrets = {
    "pagerduty-key" = var.pagerduty_routing_key
  }
}
```

{{< /tab >}}
{{< tab "Microsoft Teams" >}}

```hcl
module "monitoring" {
  # ...

  alerting = {
    receivers = {
      teams = {
        class = ["high", "normal", "low"]
        config = {
          msteamsv2_configs = [{
            webhook_url_file = "/etc/alertmanager/secrets/alertmanager-receivers/teams-webhook-url"
            send_resolved    = true
          }]
        }
      }
    }
  }

  alerting_receiver_secrets = {
    "teams-webhook-url" = var.teams_webhook_url
  }
}
```

{{< /tab >}}
{{< tab "Email" >}}

```hcl
module "monitoring" {
  # ...

  alerting = {
    global = {
      smtp_smarthost          = "smtp.example.com:587"
      smtp_from               = "alertmanager@example.com"
      smtp_auth_username      = "alertmanager"
      smtp_auth_password_file = "/etc/alertmanager/secrets/alertmanager-receivers/smtp-password"
    }
    receivers = {
      ops-email = {
        class = ["high", "normal", "low"]
        config = {
          email_configs = [{
            to = "ops@example.com"
          }]
        }
      }
    }
  }

  alerting_receiver_secrets = {
    "smtp-password" = var.smtp_password
  }
}
```

{{< /tab >}}
{{< tab "Webhook" >}}

```hcl
module "monitoring" {
  # ...

  alerting = {
    receivers = {
      incidents = {
        class = ["high", "normal", "low"]
        config = {
          webhook_configs = [{
            url           = "https://incidents.example.com/hooks/alertmanager"
            send_resolved = true
            http_config = {
              authorization = {
                credentials_file = "/etc/alertmanager/secrets/alertmanager-receivers/webhook-token"
              }
            }
          }]
        }
      }
    }
  }

  alerting_receiver_secrets = {
    "webhook-token" = var.webhook_token
  }
}
```

{{< /tab >}}
{{< /tabs >}}

The module creates the `alertmanager-receivers` Secret from
`alerting_receiver_secrets`, so the plan fails when a receiver reads a key that
the map does not set. The values stay out of plan output, but they are stored in
Terraform state. To keep them out of state, see [Manage receiver
credentials](/observability/self-managed/customize-alerting/#manage-receiver-credentials).

{{< /tab >}}
{{< tab "Helm" >}}

Add a receiver to the chart's `alerting` values:

{{< tabs >}}
{{< tab "Slack" >}}

```yaml
alerting:
  receivers:
    chat:
      class: [high, normal, low]
      config:
        slack_configs:
          - channel: "#materialize-alerts"
            api_url_file: /etc/alertmanager/secrets/alertmanager-receivers/slack-url
            send_resolved: true
```

{{< /tab >}}
{{< tab "PagerDuty" >}}

```yaml
alerting:
  receivers:
    oncall:
      class: [high, normal, low]
      config:
        pagerduty_configs:
          - routing_key_file: /etc/alertmanager/secrets/alertmanager-receivers/pagerduty-key
            send_resolved: true
```

{{< /tab >}}
{{< tab "Microsoft Teams" >}}

```yaml
alerting:
  receivers:
    teams:
      class: [high, normal, low]
      config:
        msteamsv2_configs:
          - webhook_url_file: /etc/alertmanager/secrets/alertmanager-receivers/teams-webhook-url
            send_resolved: true
```

{{< /tab >}}
{{< tab "Email" >}}

```yaml
alerting:
  global:
    smtp_smarthost: smtp.example.com:587
    smtp_from: alertmanager@example.com
    smtp_auth_username: alertmanager
    smtp_auth_password_file: /etc/alertmanager/secrets/alertmanager-receivers/smtp-password
  receivers:
    ops-email:
      class: [high, normal, low]
      config:
        email_configs:
          - to: ops@example.com
```

{{< /tab >}}
{{< tab "Webhook" >}}

```yaml
alerting:
  receivers:
    incidents:
      class: [high, normal, low]
      config:
        webhook_configs:
          - url: https://incidents.example.com/hooks/alertmanager
            send_resolved: true
            http_config:
              authorization:
                credentials_file: /etc/alertmanager/secrets/alertmanager-receivers/webhook-token
```

{{< /tab >}}
{{< /tabs >}}

{{< /tab >}}
{{< /tabs >}}

For Opsgenie, Amazon SNS, and every other integration Alertmanager supports,
write `config` as the [receiver integration reference
⧉](https://prometheus.io/docs/alerting/latest/configuration/#receiver-integration-settings)
describes, using the `_file` variant of each credential field.

{{< warning >}}
Once any receiver is configured, every class the preset maps a severity to,
other than `suppressed`, must be served by at least one receiver, or the apply
fails. Under the default `important` preset those classes are `high`, `normal`,
and `low`, which is why each receiver above serves all three.
{{< /warning >}}

### Step 3. Supply the credential and apply

{{< tabs >}}
{{< tab "Terraform" >}}

1. Declare each credential as a sensitive variable, and pass it in the way you
   pass other secrets, for example through an environment variable. For the
   Slack example:

   ```hcl
   variable "slack_webhook_url" {
     type      = string
     sensitive = true
   }
   ```

   ```bash
   export TF_VAR_slack_webhook_url='<your-slack-webhook-url>'
   ```

1. Apply the configuration:

   ```bash
   terraform apply
   ```

{{< /tab >}}
{{< tab "Helm" >}}

1. Create the `alertmanager-receivers` Secret in the namespace Alertmanager runs
   in, with one key per credential file your receiver reads. For the Slack
   example, replacing `<alertmanager-namespace>` as described in [Step
   4](#step-4-confirm-where-alerts-go):

   ```bash
   kubectl --namespace <alertmanager-namespace> create secret generic alertmanager-receivers \
     --from-file=slack-url=./slack-url.txt
   ```

   In production, source this Secret from Sealed Secrets, External Secrets, or
   SOPS rather than creating it from a local file.

1. Upgrade the release with the new values.

{{< /tab >}}
{{< /tabs >}}

### Step 4. Confirm where alerts go

Replace `<alertmanager-namespace>` with the namespace Alertmanager runs in: the
release namespace, which the `monitoring` module sets from its `namespace`
(`monitoring` in the examples), or `alertmanager` under the chart's
`split-namespace` profile.

1. Check which receiver an alert of a given severity reaches:

   ```bash
   kubectl --namespace <alertmanager-namespace> exec alertmanager-0 -c alertmanager -- \
     amtool config routes test \
       --config.file=/etc/alertmanager/config/alertmanager.yml severity=critical
   ```

   The command prints the name of each receiver the alert would reach, such as
   `chat`. `mzmon-null` means the alert reaches nobody.

1. Send a test alert to confirm that the receiver delivers:

   ```bash
   kubectl --namespace <alertmanager-namespace> exec alertmanager-0 -c alertmanager -- \
     amtool alert add alertname=ReceiverTest severity=warning \
       --annotation=summary="Delivery test for the warning class"
   ```

   The test alert is a real notification to whoever the route reaches. It is
   delivered after the route's `group_wait` (30 seconds by default), and
   resolves on its own after five minutes.

{{< note >}}
The apply checks the structure of the configuration, but not the fields inside
each receiver's `config`. Alertmanager validates those when it reloads. If it
rejects a configuration, it keeps running the previous one, and the
`alertmanager_config_last_reload_successful` metric drops to `0`. Query that
metric in Grafana after changing a receiver.
{{< /note >}}

## Thresholds for alerts you build yourself {#thresholds}

If you send metrics to a platform you already run, you can build alert rules
there instead of, or alongside, the bundled ones. Decide which system owns which
alert, rather than evaluating the same thresholds in both and notifying twice.
For more details on how to set up alert rules in Datadog or Grafana, refer to:

 * [Datadog monitors](https://docs.datadoghq.com/monitors/)
 * [Grafana alerts](https://grafana.com/docs/grafana/latest/alerting/fundamentals/)

Alert rules tend to have two threshold levels, and we are going to define them as follows:
 * **Warning:** represents a call to attention to a symptom with high chances to develop into an issue.
 * **Alert:** represents an active issue that requires immediate action.

For each threshold level, use the following table as a guide to set up your own alert rules:

Metric | Warning | Alert | Description
-- | -- | -- | --
CPU | 85% | 100% | Average CPU usage for a cluster in the last *15 minutes*.
Memory | 80% | 90% | Average memory usage for a cluster in the last *15 minutes*.
Source status | - | On Change | Source status change in the last *1 minute*.
Cluster status | - | On Change | Cluster replica status change in the last *1 minute*.
Freshness | > 5s | > 1m | Average [lag behind an input](/sql/system-catalog/mz_internal/#mz_materialization_lag) in the last *15 minutes*.

### Custom Thresholds

For the following table, replace the two variables, _X_ and _Y_, by your organization and use case:

Metric | Warning | Alert | Description
-- | -- | -- | --
Latency | Avg > X | Avg > Y | Average latency in the last *15 minutes*. Where X and Y are the expected latencies in milliseconds.

## Next steps

- [Customize alerting](/observability/self-managed/customize-alerting/), to
  page on critical alerts, route alerts to the team that owns them, tune the
  bundled rules, and silence alerts during maintenance.

- [Alert Channels
  ⧉](https://materializeinc.github.io/materialize-monitoring/alerting/channels/),
  for more receiver examples, including incident-management products,
  notification templates, and links back to Grafana.
