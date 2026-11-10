---
title: "Customize alerting"
description: "How to route, tune, and silence the alerts the Self-Managed Materialize monitoring stack raises."
menu:
  main:
    parent: "monitor-sm"
    name: "Customize alerting"
    weight: 16
    identifier: "customize-alerting-sm"
---

Once you have [configured a receiver](/observability/self-managed/alerting/),
you can change which alerts reach it, send some alerts to other receivers, tune
the bundled rules, and silence alerts during maintenance.

The examples on this page set the `monitoring` module's Terraform inputs, and
require v15.0.0 or later of the Materialize Terraform Modules. With Helm, the
same settings are chart values under `alerting` and `rules`, usually the same
name in camel case, such as `alerting.timeIntervals` for `time_intervals`.
[Configuring Alerting through Terraform
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/terraform/)
lists the chart value for each.

Commands on this page use `<alertmanager-namespace>` for the namespace
Alertmanager runs in: the release namespace, which the `monitoring` module sets
from its `namespace` (`monitoring` in the examples), or `alertmanager` under the
chart's `split-namespace` profile.

## How routing works

Every bundled rule sets a `severity` label: `critical`, `warning`, or `notice`.
The selected **preset** maps each severity to a **class**, and an alert reaches
every receiver whose `class` includes it. The shipped presets express how much
the deployment depends on Materialize:

| `severity` | `critical-infrastructure` | `important` (default) | `evaluation` |
|------------|---------------------------|-----------------------|--------------|
| `critical` | `page` | `high` | `normal` |
| `warning` | `high` | `normal` | `normal` |
| `notice` | `normal` | `low` | `suppressed` |

`suppressed` notifies nobody. A suppressed alert still fires and still shows in
Alertmanager and in Grafana.

Every bundled rule also carries an `audience` label, which you can route on:
`platform` for the Materialize deployment, its system clusters, and the
Kubernetes platform under it, or `workload` for what runs on it, such as a user
cluster falling behind or running out of memory.

## Page on critical alerts

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
`platform` receiver gets both. The `route` options on `oncall` apply only to
alerts routed to it, so pages are grouped and repeated faster than anything
else.

### Define your own preset

To map severities to classes of your own, add an entry under `presets` and
select it with `preset`:

```hcl
module "monitoring" {
  # ...

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
            url = "https://tickets.example.com/hooks/alertmanager"
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

  alerting_receiver_secrets = {
    "opsgenie-key"  = var.opsgenie_api_key
    "tickets-token" = var.tickets_token
  }
}
```

A cell set under `presets` for a shipped preset name, such as `important`,
overrides only that cell and keeps the rest. An alert whose `severity` label is
missing or unknown is routed as `unknown_severity`, which defaults to
`warning`.

## Route alerts to the team that owns them

`routes.extra` takes routes in Alertmanager's own format, and places them ahead
of the preset's severity routes, so a specific match wins and the preset remains
the fallback. This example sends every `workload` alert to the team that owns
the clusters:

```hcl
module "monitoring" {
  # ...

  alerting = {
    receivers = {
      chat = {
        class = ["high", "normal", "low"]
        config = {
          slack_configs = [{
            channel      = "#materialize-alerts"
            api_url_file = "/etc/alertmanager/secrets/alertmanager-receivers/slack-url"
          }]
        }
      }
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
        matchers = ["audience=workload"]
        receiver = "data-team"
        continue = true
      }]
    }
  }

  alerting_receiver_secrets = {
    "slack-url"           = var.slack_webhook_url
    "data-team-slack-url" = var.data_team_slack_webhook
  }
}
```

`data-team` has no `class`, so only this route reaches it. Because the route
sets `continue = true`, a workload alert also continues through the preset to
`chat`. Without it, the alert would go to `data-team` only. Each receiver an
extra route names must be defined under `receivers`, or the apply fails.

## Tune the bundled rules

`alert_rules` sets which bundled rules install, and adjusts them without changing
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
| `selected` | Rules to install beyond the default set: alert names, rule-group names, or `"*"` for every rule that applies. Evaluate a rule outside the default set against your deployment before relying on it. |
| `disabled` | Alert names never to install. |
| `overrides` | Per alert, `for_duration` (the rule's `for`) and `labels`. An override's `severity` must stay `critical`, `warning`, or `notice`, and its `audience` `platform` or `workload`, so the routes still match it. |
| `excluded_namespaces` | Namespaces no rule alerts on. |

For the remaining attributes, including the namespaces your Materialize
environments run in and the infrastructure workload tiers, see [Tuning the
bundled rules
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/terraform/#tuning-the-bundled-rules).
For every bundled rule and whether it is in the default set, see [Common Alerts
⧉](https://materializeinc.github.io/materialize-monitoring/reference/common-alerts/).

{{< note >}}
The rule evaluator imports every `PrometheusRule` in the cluster, not only the
bundled ones. If another chart in the cluster ships its own `PrometheusRule`
resources, those alerts are also evaluated and notified through this
Alertmanager.
{{< /note >}}

## Silence alerts

A Materialize upgrade restarts pods and rehydrates clusters, which can fire
alerts that resolve on their own. A silence stops the notifications for alerts
matching a set of labels until it expires. The alerts still fire and still show
in Alertmanager and Grafana.

{{< tabs >}}
{{< tab "Grafana" >}}

In Grafana, go to **Alerting > Silences**, select the **Alertmanager** data
source, and create a silence.

{{< /tab >}}
{{< tab "amtool" >}}

```bash
kubectl --namespace <alertmanager-namespace> exec alertmanager-0 -c alertmanager -- \
  amtool silence add namespace=materialize-environment \
    --duration=2h --author="$USER" --comment="Planned Materialize upgrade"
```

The command has two namespaces in it. `--namespace` is where Alertmanager runs,
and `namespace=materialize-environment` matches the alerts to silence, here
those about the Materialize environment's namespace.

`amtool silence query` lists active silences, and `amtool silence expire <id>`
ends one early.

{{< /tab >}}
{{< /tabs >}}

Silences are shared between the Alertmanager replicas and survive the loss of
either one. Give each a duration that covers the work and no more, so it does
not hide the next incident on the same labels.

For a recurring schedule, define it under `time_intervals` and reference it from
a receiver's `route.mute_time_intervals`:

```hcl
module "monitoring" {
  # ...

  alerting = {
    time_intervals = [{
      name = "change-window"
      time_intervals = [{
        weekdays = ["saturday"]
        times    = [{ start_time = "02:00", end_time = "06:00" }]
        location = "America/New_York"
      }]
    }]

    receivers = {
      chat = {
        class = ["high", "normal", "low"]
        route = { mute_time_intervals = ["change-window"] }
        config = {
          slack_configs = [{
            channel      = "#materialize-alerts"
            api_url_file = "/etc/alertmanager/secrets/alertmanager-receivers/slack-url"
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

A mute window mutes everything routed to that receiver during the window,
including an unrelated incident. For inhibition rules and the other options, see
[Maintenance Windows
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/maintenance/).

## Manage receiver credentials

When `alerting_receiver_secrets` is set, the module creates the
`alertmanager-receivers` Secret from it:

- The plan fails when a receiver reads a key the map does not set, and the error
  names the key.

- The values stay out of plan output, because the input is `sensitive`, but they
  are stored in Terraform state. Restrict who can read your state accordingly.

- Rotating a credential needs no restart. Alertmanager reads the file on every
  send, and the kubelet refreshes the mounted Secret within about a minute.

Amazon SNS is the only integration that authenticates with the pod's own cloud
identity rather than a credential. Every other integration reads a credential
from the Secret, including email sent through Amazon SES or Azure Communication
Services. For the setup of each, see [Cloud provider services
⧉](https://materializeinc.github.io/materialize-monitoring/alerting/channels/#cloud).

### Manage the Secret outside Terraform

To keep receiver credentials out of Terraform state, leave
`alerting_receiver_secrets` empty. The module then creates no Secret, so
External Secrets Operator, Vault Agent, or a cloud secret store's CSI driver can
own `alertmanager-receivers` instead. Create it in the namespace Alertmanager
runs in. Alertmanager mounts it as optional, so its pods start before the Secret
exists.

{{< warning >}}
With no `alerting_receiver_secrets`, the plan no longer checks that the Secret
holds every key your receivers read. A missing key fails only when Alertmanager
next tries to send through that receiver.
{{< /warning >}}

### Run Alertmanager in its own namespace

The chart's `split-namespace` profile runs Alertmanager in a namespace of its
own, `alertmanager`. When you apply that profile through `additional_values`,
also set `alertmanager_namespace = "alertmanager"` so the Secret is created
where Alertmanager can mount it. The module cannot infer this from the profile.
Commands that reach Alertmanager, such as `amtool`, then run in the
`alertmanager` namespace.

## Troubleshooting

Most configuration mistakes fail before anything is installed. The rest fail the
Helm render during the apply, or are rejected by Alertmanager itself:

| Mistake | Fails at |
|---------|----------|
| A receiver reads a key that `alerting_receiver_secrets` does not set | Plan, naming the key. Only when `alerting_receiver_secrets` is set |
| An override's `for_duration` is not a duration | Plan |
| `preset` is not a shipped preset or a key of `presets` | Plan |
| A template name does not end in `.tmpl` | Plan |
| An unknown capability, alert, or rule-group name | Apply |
| An inline credential in a receiver or in `global` | Apply |
| Once any receiver is configured, the preset maps a severity to a class no receiver serves | Apply |
| An extra route names an undefined receiver or time interval | Apply |
| An invalid field inside a receiver's `config` | Alertmanager reload. Alertmanager keeps the previous configuration, and `alertmanager_config_last_reload_successful` drops to `0` |

`terraform plan` shows the composed Helm values in the monitoring module's
`helm_release` resource, with `alerting_receiver_secrets` shown as `(sensitive
value)`. To check where an alert is routed once applied, see [Confirm where
alerts go](/observability/self-managed/alerting/#step-4-confirm-where-alerts-go).

## See also

- [Alerting](/observability/self-managed/alerting/), to configure your first
  receiver.

- [Grafana](/observability/self-managed/grafana/), where firing alerts and
  silences are visible alongside the dashboards.

- [Configuring Alerting through Terraform
  ⧉](https://materializeinc.github.io/materialize-monitoring/alerting/terraform/),
  the full reference for the Terraform inputs.

- [Alert Channels
  ⧉](https://materializeinc.github.io/materialize-monitoring/alerting/channels/),
  for more receiver examples and the chart's routing values.
