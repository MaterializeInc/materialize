---
title: "Alerting"
description: "How to route the alerts the Self-Managed Materialize monitoring stack raises to PagerDuty, Slack, email, and other receivers, and which thresholds to alert on in another platform."
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
**Alertmanager**. This guide walks you through telling Alertmanager where to
send those alerts, such as PagerDuty, Slack, Opsgenie, Microsoft Teams, email,
or a webhook, and tuning which rules install.

{{< warning >}}
A default install configures no receiver. Until you configure one, every alert
is routed to `mzmon-null`, which notifies nobody.
{{< /warning >}}

If you alert from a platform you already run instead, such as Datadog or
Honeycomb, see [Thresholds for alerts you build yourself](#thresholds) for the
metrics and thresholds to start from.

## How it works

Two rule evaluators run in the `monitoring` namespace, and both send what fires
to the same Alertmanager:

| Evaluator | Evaluates | Against |
|-----------|-----------|---------|
| Thanos Ruler | PromQL rules, installed as `PrometheusRule` resources | The bundled Thanos |
| Loki Ruler | LogQL rules | The bundled Loki |

The chart ships alert rules for Materialize and for the Kubernetes platform
under it. A **default set** installs automatically: the rules that have been
checked against a live Self-Managed deployment without firing falsely. The rest
are opt-in, and you can [select them](#how-to-tune-the-bundled-rules). For every
bundled rule and what it detects, see [Common Alerts
⧉](https://materializeinc.github.io/materialize-monitoring/reference/stable-metrics/common-alerts/).

{{< note >}}
The Thanos Ruler imports every `PrometheusRule` in the cluster, not only the
bundled ones. If another chart in the cluster ships its own `PrometheusRule`
resources, those alerts are also evaluated and notified through this
Alertmanager.
{{< /note >}}

### How an alert finds its receiver

Three settings decide where an alert goes:

| Setting | Owned by | Values |
|---------|----------|--------|
| **Severity**, how bad a condition is | Each rule's `severity` label | `critical`, `warning`, or `notice` |
| **Preset**, what each severity means for this deployment | You, with `preset` | `critical-infrastructure`, `important` (the default), `evaluation`, or [your own](#how-to-define-your-own-preset) |
| **Class**, a kind of delivery, such as a page or a low-priority notice | You, on each receiver | Any name. The shipped presets use `page`, `high`, `normal`, `low`, and `suppressed` |

A preset maps each severity to a class, and an alert reaches every receiver that
serves its class. The shipped presets express how much the deployment depends on
Materialize:

| `severity` | `critical-infrastructure` | `important` (default) | `evaluation` |
|------------|---------------------------|-----------------------|--------------|
| `critical` | `page` | `high` | `normal` |
| `warning` | `high` | `normal` | `normal` |
| `notice` | `normal` | `low` | `suppressed` |

`suppressed` notifies nobody. A suppressed alert still fires and still shows in
Alertmanager and in Grafana.

Every bundled rule also carries an `audience` label, so you can route on who
acts on it:

| `audience` | Covers |
|------------|--------|
| `platform` | The Materialize deployment, its system clusters, and the Kubernetes platform under it |
| `workload` | What runs on the deployment, such as a user cluster falling behind, stuck hydrating, or running out of memory |

## Instructions

### Before you begin

Ensure you have:

- A Materialize deployment created with the [Materialize Terraform
  modules](/self-managed-deployments/), with the monitoring stack enabled. See
  [Step 1](#step-1-enable-observability).

- [Terraform ⧉](https://developer.hashicorp.com/terraform/install) installed.

- [kubectl ⧉](https://kubernetes.io/docs/tasks/tools/) installed and configured
  to connect to your cluster.

- The credential for each receiver you plan to use, such as a PagerDuty routing
  key or a Slack incoming webhook URL.

{{< note >}}
The Terraform steps on this page require **v16.0.0** or later of the
Materialize Terraform Modules, which is where the `monitoring` module accepts
the alerting inputs. Upgrading to v16.0.0 does not change alerting on its own:
every alerting input defaults to empty, so a deployment that sets none of them
renders exactly as before.

If you install the `materialize-monitoring` chart with Helm rather than through
the Terraform modules, follow the [Helm
instructions](#instructions-when-using-helm) instead.
{{< /note >}}

### Step 1. Enable observability

{{% include-headless "/headless/monitoring/enable-observability" %}}

### Step 2. Configure a receiver

Alerting is configured on the `monitoring` module block, not through a root
variable of the examples. It takes four inputs:

| Input | Configures |
|-------|------------|
| `alerting` | Where alerts go: the preset, receivers, routes, inhibit rules, time intervals, notification templates, and Alertmanager's `global` block |
| `alerting_receiver_secrets` | Receiver credentials. Sensitive, and created as the `alertmanager-receivers` Kubernetes Secret rather than passed through the Helm values. See [How receiver credentials are handled](#how-receiver-credentials-are-handled) |
| `alert_rules` | Which bundled rules install, and how they are tuned. See [How to tune the bundled rules](#how-to-tune-the-bundled-rules) |
| `alertmanager_namespace` | The namespace the Secret is created in. Defaults to the module's `namespace` |

1. In the `monitoring` module block of your Terraform, add a receiver and the
   credential it reads. This example sends every alert to one Slack channel,
   under the default `important` preset:

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

   The examples ship an `alerting` block commented out in the `monitoring`
   module, so you can uncomment it in place.

   | Receiver attribute | Purpose |
   |--------------------|---------|
   | `class` | The classes this receiver serves, as a string or a list. A receiver with no `class` is reachable only from an [extra route](#how-to-route-workload-alerts-to-the-team-that-owns-them). |
   | `config` | An Alertmanager receiver, written as HCL: `slack_configs`, `pagerduty_configs`, `opsgenie_configs`, `msteamsv2_configs`, `email_configs`, `webhook_configs`, `sns_configs`, or any other integration Alertmanager supports. Field names are Alertmanager's own. See the [receiver integration reference ⧉](https://prometheus.io/docs/alerting/latest/configuration/#receiver-integration-settings). |
   | `route` | Optional. Route options applied wherever the preset routes to this receiver, such as `group_wait`, `repeat_interval`, and `mute_time_intervals`. |

   {{< warning >}}
   Every class the selected preset maps a severity to, other than `suppressed`,
   must be served by at least one receiver, or the apply fails. Under the
   default `important` preset those classes are `high`, `normal`, and `low`,
   which is why the receiver above lists all three.
   {{< /warning >}}

1. Declare the credential as a sensitive variable and pass it in the way you
   pass other secrets, for example through an environment variable:

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

### Step 3. Confirm where alerts go

1. Check which receiver an alert of a given severity reaches:

   ```bash
   kubectl --namespace monitoring exec alertmanager-0 -c alertmanager -- \
     amtool config routes test \
       --config.file=/etc/alertmanager/config/alertmanager.yml severity=critical
   ```

   The command prints the name of each receiver the alert would reach, such as
   `chat`. `mzmon-null` means the alert reaches nobody.

1. Send a test alert to confirm that the receiver delivers:

   ```bash
   kubectl --namespace monitoring exec alertmanager-0 -c alertmanager -- \
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

## How receiver credentials are handled

A receiver's credentials never go in `alerting`. Anything in the Helm values is
readable with `helm get values`, and the apply fails on an inline credential.

Instead, a receiver reads its credential from a file, through the `_file`
variant of the field. Each key of `alerting_receiver_secrets` becomes a key of
the `alertmanager-receivers` Secret, which is mounted at
`/etc/alertmanager/secrets/alertmanager-receivers/`. So the key `slack-url` is
read as `/etc/alertmanager/secrets/alertmanager-receivers/slack-url`.

| Integration | Instead of | Use |
|-------------|------------|-----|
| Slack | `api_url` | `api_url_file` |
| PagerDuty | `routing_key`, `service_key` | `routing_key_file`, `service_key_file` |
| Opsgenie | `api_key` | `api_key_file` |
| Microsoft Teams | `webhook_url` | `webhook_url_file` |
| Email | `auth_password`, or `smtp_auth_password` in `global` | `auth_password_file`, or `smtp_auth_password_file` |
| Webhook, and any `http_config` | `authorization.credentials`, `basic_auth.password` | `credentials_file`, `password_file` |
| Webhook whose URL carries a token | `url` | `url_file` |

- **The plan fails** when a receiver reads a key that `alerting_receiver_secrets`
  does not set, and the error names the key.

- **Rotating a credential needs no restart.** Alertmanager reads the file on
  every send, and the kubelet refreshes the mounted Secret within about a
  minute.

- **Values stay out of plan output**, because `alerting_receiver_secrets` is
  `sensitive`. They are still stored in Terraform state, as with every Secret
  Terraform manages, so restrict who can read your state accordingly.

Amazon SNS is the only integration that authenticates with the pod's own cloud
identity rather than a credential. Every other integration reads a credential
from the Secret, including email sent through Amazon SES or Azure Communication
Services. For the setup of each, see [Cloud provider services
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/channels/#cloud).

### Manage the Secret outside Terraform

To keep receiver credentials out of Terraform state, leave
`alerting_receiver_secrets` empty. The module then creates no Secret and skips
the plan-time key check, so External Secrets Operator, Vault Agent, or a cloud
secret store's CSI driver can own `alertmanager-receivers` instead. Create it in
the namespace Alertmanager runs in, `monitoring` by default. Alertmanager mounts
it as optional, so its pods start before the Secret exists.

### Run Alertmanager in its own namespace

The chart's `split-namespace` profile runs Alertmanager in a namespace of its
own, `alertmanager`. When you apply that profile through `additional_values`,
also set `alertmanager_namespace = "alertmanager"` so the Secret is created
where Alertmanager can mount it. The module cannot infer this from the profile.

## How to page on critical alerts

The `critical-infrastructure` preset maps `critical` to `page`. This example
pages through PagerDuty and sends everything else to Slack:

```hcl
module "monitoring" {
  # ...

  alerting = {
    preset = "critical-infrastructure"

    receivers = {
      oncall = {
        class = "page"
        route = { group_wait = "10s", repeat_interval = "1h" }
        config = {
          pagerduty_configs = [{
            routing_key_file = "/etc/alertmanager/secrets/alertmanager-receivers/pagerduty-key"
            send_resolved    = true
          }]
        }
      }
      platform = {
        class = ["high", "normal"]
        config = {
          slack_configs = [{
            channel       = "#platform-alerts"
            api_url_file  = "/etc/alertmanager/secrets/alertmanager-receivers/slack-url"
            send_resolved = true
          }]
        }
      }
    }
  }

  alerting_receiver_secrets = {
    "pagerduty-key" = var.pagerduty_routing_key
    "slack-url"     = var.platform_slack_webhook
  }
}
```

Under this preset `warning` maps to `high` and `notice` to `normal`, so the
`platform` receiver gets both. The `route` options on `oncall` apply only where
the preset routes to it, so pages are grouped and repeated faster than anything
else.

### How to define your own preset

To map severities to classes of your own, add an entry under `presets` and
select it with `preset`:

```hcl
alerting = {
  preset = "oncall-lite"
  presets = {
    oncall-lite = {
      critical = "page"
      warning  = "ticket"
      notice   = "suppressed"
    }
  }

  receivers = {
    oncall = {
      class = "page"
      config = {
        opsgenie_configs = [{
          api_key_file = "/etc/alertmanager/secrets/alertmanager-receivers/opsgenie-key"
        }]
      }
    }
    tickets = {
      class = "ticket"
      config = {
        webhook_configs = [{
          url = "https://tickets.example.internal/hooks/alertmanager"
          http_config = {
            authorization = {
              credentials_file = "/etc/alertmanager/secrets/alertmanager-receivers/tickets-token"
            }
          }
        }]
      }
    }
  }
}
```

A cell set under `presets` for a shipped preset name, such as `important`,
overrides only that cell and keeps the rest. An alert whose `severity` label is
missing or unknown is routed as `unknown_severity`, which defaults to
`warning`.

## How to route workload alerts to the team that owns them

`routes.extra` takes routes in Alertmanager's own format and places them ahead
of the preset's severity routes, so a specific match wins and the preset
remains the fallback. This example sends every `workload` alert to the team
that owns the clusters, as well as through the preset:

```hcl
alerting = {
  # ...preset and the other receivers...

  receivers = {
    # ...
    data-team = {
      config = {
        slack_configs = [{
          channel      = "#data-platform"
          api_url_file = "/etc/alertmanager/secrets/alertmanager-receivers/data-team-slack-url"
        }]
      }
    }
  }

  routes = {
    extra = [{
      matchers = ["audience=\"workload\""]
      receiver = "data-team"
      continue = true
    }]
  }
}
```

A matcher is an Alertmanager string, so the quotes inside it are escaped in HCL.
`data-team` has no `class`, so it is reachable only from this route.

| `continue` | An alert the route matches |
|------------|----------------------------|
| `false` (default) | Goes to this route's receiver only |
| `true` | Goes to this route's receiver, then on through the preset as well |

Each receiver an extra route names must be defined under `receivers`, or the
apply fails.

## How to tune the bundled rules

`alert_rules` sets which bundled rules install and adjusts them without changing
their expressions:

```hcl
module "monitoring" {
  # ...

  alert_rules = {
    # User-cluster freshness is opt-in, because some clusters are behind by design.
    selected = ["cluster-falling-behind", "cluster-stale"]
    disabled = ["pods-stuck-in-waiting"]

    overrides = {
      # Large clusters here take hours to hydrate with nothing wrong.
      cluster-hydration-stuck   = { for_duration = "6h" }
      cluster-replica-oomkilled = { labels = { severity = "notice" } }
    }

    excluded_namespaces = ["materialize-scratch"]
  }
}
```

| Attribute | Purpose |
|-----------|---------|
| `enabled` | `false` installs none of the bundled rules. |
| `selected` | Rules to install beyond the default set: alert names, rule-group names, or `"*"` for every rule that applies. Evaluate a rule outside the default set against your deployment before relying on it. |
| `disabled` | Alert names never to install. |
| `overrides` | Per alert, `for_duration` (the rule's `for`) and `labels`. An override never changes an expression. |
| `capabilities` | Components your deployment runs beyond those the chart derives, such as `cert-manager` or `coredns`. A rule installs only when every capability it requires is present. |
| `environment_namespaces` | The namespaces your Materialize environments run in. Defaults to the module's `materialize_instance_namespace`. Rules about an environment's pods match only these namespaces, so set it if you run environments elsewhere. |
| `excluded_namespaces` | Namespaces no rule alerts on. |
| `infra_workloads` | Which infrastructure workloads count as `core`, `important`, `nonessential`, or `daemonset`. Read only by rules outside the default set. |

An override is named `for_duration` because HCL reads `{ for = "6h" }` as a
`for` expression. An override's `severity` label must stay `critical`,
`warning`, or `notice`, and its `audience` label `platform` or `workload`, so
the routes still match it.

## How to silence alerts during maintenance

A Materialize upgrade restarts pods and rehydrates clusters, which can fire
alerts that resolve on their own. A silence stops the notifications for alerts
matching a set of labels until it expires, while the alerts still fire and still
show in Alertmanager and Grafana.

- **In Grafana**, go to **Alerting > Silences**, select the **Alertmanager**
  data source, and create a silence.

- **With `amtool`**:

  ```bash
  kubectl --namespace monitoring exec alertmanager-0 -c alertmanager -- \
    amtool silence add namespace=materialize-environment \
      --duration=2h --author="$USER" --comment="Planned Materialize upgrade"
  ```

  `amtool silence query` lists active silences, and `amtool silence expire <id>`
  ends one early.

Silences are shared between the Alertmanager replicas and survive the loss of
either one. Give each a duration that covers the work and no more, so it does
not hide the next incident on the same labels.

For a recurring schedule, define it under `time_intervals` and reference it from
a receiver's `route.mute_time_intervals`:

```hcl
alerting = {
  # ...

  time_intervals = [{
    name = "change-window"
    time_intervals = [{
      weekdays = ["saturday"]
      times    = [{ start_time = "02:00", end_time = "06:00" }]
      location = "America/New_York"
    }]
  }]

  receivers = {
    platform = {
      class = ["high", "normal"]
      route = { mute_time_intervals = ["change-window"] }
      config = {
        # ...
      }
    }
  }
}
```

A mute window mutes everything the route delivers during the window, including
an unrelated incident. For inhibition rules and the other options, see
[Maintenance Windows
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/maintenance/).

## Troubleshooting

Most configuration mistakes fail before anything is installed, and the rest
fail the Helm render during the apply:

| Mistake | Fails at |
|---------|----------|
| A receiver reads a key that `alerting_receiver_secrets` does not set | Plan, naming the key |
| An override's `for_duration` is not a duration | Plan |
| `preset` is not a shipped preset or a key of `presets` | Plan |
| A template name does not end in `.tmpl` | Plan |
| An unknown capability, alert, or rule-group name | Apply |
| An inline credential in a receiver or in `global` | Apply |
| The preset maps a severity to a class no receiver serves | Apply |
| An extra route names an undefined receiver or time interval | Apply |

`terraform plan` shows the composed Helm values in the monitoring module's
`helm_release` resource, with `alerting_receiver_secrets` shown as `(sensitive
value)`.

## Instructions when using Helm

If you install the `materialize-monitoring` chart directly rather than through
the Terraform modules, alerting is configured under the chart's `alerting` and
`rules` values, and you create the receiver Secret yourself.

1. Configure a receiver:

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

1. Create the `alertmanager-receivers` Secret in the namespace Alertmanager runs
   in, with one key per credential file:

   ```bash
   kubectl --namespace monitoring create secret generic alertmanager-receivers \
     --from-file=slack-url=./slack-url.txt
   ```

   {{< warning >}}
   In production, source this Secret from Sealed Secrets, External Secrets, or
   SOPS rather than creating it from a local file.
   {{< /warning >}}

Each Terraform attribute on this page has a chart value under `alerting` or
`rules`, usually the same name in camel case, such as `alerting.timeIntervals`
for `time_intervals`. [Configuring Alerting through Terraform
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/terraform/)
lists the chart value for each. For the full value reference, see [Alert
Channels
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/channels/)
and [Configuring Alerting
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/configuring/).

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
Credits | Consumption rate increase by X% | Consumption rate increase by Y% | Average credit consumption in the last *60 minutes*.

## See also

- [Grafana](/observability/self-managed/grafana/), where firing alerts and
  silences are visible alongside the dashboards.

- [How logs and metrics are stored](/observability/self-managed/storage/), for
  the stores the rules evaluate against.

- [Configuring Alerting through Terraform
  ⧉](https://materializeinc.github.io/materialize-monitoring/alerting/terraform/),
  the full reference for the four Terraform inputs.

- [Alert Channels
  ⧉](https://materializeinc.github.io/materialize-monitoring/alerting/channels/),
  for more receiver examples, including email, incident-management products,
  notification templates, and links back to Grafana.
