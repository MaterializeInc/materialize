---
title: "Observability tools"
description: "How to read the Self-Managed Materialize monitoring stack from a browser, an AI agent, or the command line."
menu:
  main:
    parent: "monitor-sm"
    weight: 2
    identifier: "o11y-tools-sm"
---

The monitoring stack that the Materialize Terraform modules install keeps
metrics in Thanos and logs in Loki, and serves both through Grafana. You can
read that data in a browser, through an AI agent, or from the command line.
This page compares those tools and shows how to set each one up.

This page assumes the monitoring stack is already running and that you can
reach Grafana. To enable the stack and sign in to Grafana, see
[Grafana](/observability/self-managed/grafana/).

## Compare the tools

Every tool reads the same metrics, logs, and alerts. The tools differ in who
they suit, what access they need, and how much they ask of you:

| | [Grafana](#use-grafana-in-a-browser) | [Grafana MCP server](#connect-an-ai-agent) | [`gcx`](#use-gcx-from-the-command-line) |
|---|---|---|---|
| **Interface** | Browser | AI agent, over MCP | Command line |
| **Audience** | Everyone who reads dashboards | Anyone who works with an AI agent | Scripts, and agents that run shell commands |
| **Grafana access** | Browser login | Service account token | Service account token |
| **Ease of use** | Highest: nothing to install | High after setup, but check its answers | Moderate: PromQL and LogQL |
| **Stability** | Stable | Fast-moving: tools change in minor releases | Stable within a major version |

All three reach the data through Grafana. None of them needs Kubernetes access
unless you can't reach Grafana's address directly, for example because its load
balancer is internal and you are outside the network. In that case, port-forward
Grafana as [Access Grafana](/observability/self-managed/grafana/#step-2-access-grafana)
describes.

Pick a tool by what you are doing:

- **Reading a dashboard, or sharing a link:** use [Grafana in a
  browser](#use-grafana-in-a-browser).
- **Asking questions in plain language, or having an AI agent investigate:**
  use the [Grafana MCP server](#connect-an-ai-agent).
- **Scripting queries, or working from a terminal:** use
  [`gcx`](#use-gcx-from-the-command-line).

To query Thanos, Loki, or Alertmanager directly, for example while Grafana is
down, see [Backend APIs
⧉](https://materializeinc.github.io/materialize-monitoring/o11y-tools/backend-apis/)
in the monitoring stack's documentation.

{{< warning >}}
Every tool on this page reads all of the telemetry the stack holds. A Grafana
role limits what you can change, not what you can read, so any Grafana login or
service account token reads every metric and every log.
{{< /warning >}}

## Names the tools share

Scripts and AI agents reach the data through Grafana's data sources, by UID:

| Data source | Type | UID | Holds |
|-------------|------|-----|-------|
| Thanos | `prometheus` | `mzmon-thanos` | Metrics |
| Loki | `loki` | `mzmon-loki` | Logs and Kubernetes events |
| Alertmanager | `alertmanager` | `mzmon-alertmanager` | Firing alerts and silences |

The following names also come up often:

- **Dashboard UIDs** start with `mz-mon-`. For example, `mz-mon-env-top` is
  the **Materialize Environment Overview** dashboard.

- **Folder UIDs** are `materialize` for the Materialize dashboards and `infra`
  for the cluster dashboards.

- **The environment label**, `materialize_cloud_organization_name`, scopes
  Materialize metrics to one environment. Self-Managed deployments carry it
  too, despite its name.

## Use Grafana in a browser

Grafana needs no setup beyond signing in. For its address and the `admin`
password, see [Access Grafana](/observability/self-managed/grafana/#step-2-access-grafana).

Start from these places:

- **Dashboards:** the **Materialize** folder holds the per-environment
  dashboards. Start with **Materialize Environment Overview**.

- **Explore:** query the **Thanos** data source in PromQL, or the **Loki** data
  source in LogQL.

- **Alerting:** **Alerting > Silences** shows and manages silences in the
  bundled Alertmanager. See [Silence
  alerts](/observability/self-managed/customize-alerting/#silence-alerts).

A dashboard's URL carries its time range and variables, so you can share what
you see with anyone who can sign in.

## Create a service account token

The Grafana MCP server and `gcx` sign in to Grafana with a service account
token rather than a password. To create one:

1. Sign in to Grafana as `admin`.

1. Go to **Administration > Users and access > Service accounts**, and click
   **Add service account**.

1. Enter a display name, such as `mcp-yourname`, and click **Create**.

1. Check that the service account's role is **Viewer**, and change it with the
   role picker if it is not.

1. Click **Add service account token**, then **Generate token**. Copy the
   token, and store it somewhere safe.

Create one service account for each person or agent, so you can revoke one
without the rest. The **Viewer** role is enough to read dashboards and run
queries.

{{< note >}}
Grafana keeps service accounts and tokens in its own database. If your
deployment still keeps that database on the chart's default SQLite, every
token disappears when Grafana restarts. See [Persist Grafana's own
state](/observability/self-managed/grafana/#step-3-persist-grafanas-own-state).
{{< /note >}}

## Connect an AI agent

The [Grafana MCP server
⧉](https://grafana.com/docs/grafana/latest/ai-tools/mcp/set-up/),
`mcp-grafana`, connects an AI agent such as Claude Code, Claude Desktop, or
Cursor to Grafana over the Model Context Protocol (MCP). The agent can then
search and read dashboards, run PromQL and LogQL through Grafana's data sources,
and read alert rules.

The Grafana MCP server reads the monitoring stack. To let an agent read
Materialize's own system catalog instead, see [MCP Server for
developer](/developer-tools/mcp-server/mcp-developer/). An agent can use both.

{{< note >}}
The Materialize dashboards use Grafana's v2 dashboard schema, which
`mcp-grafana` reads and writes natively from version 0.16.0. Use a 2.x release,
which fixes several v2 bugs. An older version converts each dashboard to the v1
schema before the agent sees it, and drops the panel queries in v2-only
layouts.
{{< /note >}}

### Set up the server

1. [Create a service account token](#create-a-service-account-token).

1. Install [uv ⧉](https://docs.astral.sh/uv/getting-started/installation/),
   which downloads and runs `mcp-grafana` on demand. For the container image,
   release binaries, and Helm chart, see [Set up the Grafana MCP server
   ⧉](https://grafana.com/docs/grafana/latest/ai-tools/mcp/set-up/).

1. Add the server to your agent. For Claude Code, run:

   ```bash
   claude mcp add-json grafana '{
     "command": "uvx",
     "args": ["mcp-grafana", "--disable-write"],
     "env": {
       "GRAFANA_URL": "<grafana-url>",
       "GRAFANA_SERVICE_ACCOUNT_TOKEN": "<service-account-token>"
     }
   }'
   ```

   For Claude Desktop, Cursor, and other agents, put the same `command`,
   `args`, and `env` in the agent's MCP configuration.

Replace the following:

- `<grafana-url>`: Grafana's address, from `terraform output -raw
  grafana_url`. If you port-forward Grafana instead, use
  `http://localhost:3000`, and keep the port-forward running while the agent
  works.

- `<service-account-token>`: the token you created. To keep it out of the
  agent's configuration, set `GRAFANA_SERVICE_ACCOUNT_TOKEN_FILE` to the path
  of a file that holds it instead.

`mcp-grafana` reports anonymous usage statistics to Grafana Labs by default:
which tools ran and how the server is configured. The reports never include
queries, log lines, credentials, or your Grafana's address. To turn them off,
add `--usage-stats=disabled` to `args`.

### Keep the agent read-only

`--disable-write` removes the tools that create, update, or delete anything,
such as dashboards and annotations, and limits the generic `grafana_api_request`
tool to reads. Keep it on for investigation.

Log lines reach the agent as tool output, and a log line can carry text that
reads like an instruction. With `--disable-write`, an agent that follows one
cannot change anything through the server.

Grafana Operator re-pushes the Materialize dashboards from the released chart,
so an agent's edit to one does not last. To customize a dashboard, copy it to
a new UID first.

### What the agent can do

The following parts of `mcp-grafana` work against the bundled Grafana:

- **Dashboards:** find, summarize, and read dashboards, including the queries
  behind each panel.
- **Prometheus:** run PromQL against the Thanos data source, and list metrics
  and labels.
- **Loki:** run LogQL against the Loki data source, list labels, and read stream
  statistics.
- **Alerting:** list alert rules and their state.
- **Navigation:** build a Grafana link to a dashboard, a panel, or an Explore
  query, so you can check the agent's answer in a browser.

The tools that render a panel as an image need the Grafana Image Renderer,
which the stack does not run, so they fail.

### Give the agent context

The agent knows Grafana, not Materialize. Point it at the [names the tools
share](#names-the-tools-share) and at the documentation it can read:

- [`llms.txt` ⧉](https://materialize.com/docs/llms.txt) indexes the Materialize
  documentation, including this page.
- [`llms.txt` for the monitoring stack
  ⧉](https://materializeinc.github.io/materialize-monitoring/llms.txt) indexes
  the monitoring stack's own documentation: its dashboards, queries, and
  troubleshooting guides.

The following prompts are places to start:

- "Open the `mz-mon-env-top` dashboard in Grafana and summarize the health of
  environment `<environment>`."
- "Which Materialize clusters have used more than 80% of their memory limit in
  the last 6 hours? Show me the PromQL you ran."
- "Find error lines from `environmentd` in the last hour, group them by
  message, and give me an Explore link for each group."

Replace `<environment>` with the environment's
`materialize_cloud_organization_name` value.

### Check the agent's answers

An agent reads the same data you do, and can misread it in the same ways:

- **Ask for the query.** A PromQL or LogQL expression, with its time range, is
  something you can run again. A summary is not.
- **Ask for a link,** so you can see the panel the agent read.
- **Confirm a label before building on it.** A query that filters on a label
  the series does not carry returns nothing, which looks healthy.
- **Don't read quiet as healthy.** Only a default set of the bundled alert rules
  is installed, so no firing alerts does not mean nothing is wrong.

## Use gcx from the command line

[`gcx` ⧉](https://grafana.com/docs/grafana/latest/ai-tools/grafana-cli/gcx/) is
Grafana Labs' command-line tool. It reaches dashboards, data sources, and
alerting through Grafana's API, so it needs a service account token and no
Kubernetes access. An AI agent that can run shell commands can use it too.

1. Install `gcx`, following [Install gcx
   ⧉](https://grafana.com/docs/grafana/latest/ai-tools/grafana-cli/gcx/installation/).

1. [Create a service account token](#create-a-service-account-token).

1. Log in, and name the connection `mzmon`:

   ```bash
   gcx login mzmon --server <grafana-url> --token <service-account-token>
   ```

1. Set the stack's data sources as the defaults, so queries don't need a `-d`
   flag:

   ```bash
   gcx config set contexts.mzmon.datasources.prometheus mzmon-thanos
   gcx config set contexts.mzmon.datasources.loki mzmon-loki
   ```

Replace `<grafana-url>` and `<service-account-token>` as in [Set up the
server](#set-up-the-server).

Then query metrics and logs, and read dashboards:

```bash
gcx --context mzmon metrics query 'up{namespace="materialize-environment"}' --since 1h
gcx --context mzmon logs query '{namespace="materialize-environment", app="environmentd"} |= "error"' --since 1h
gcx --context mzmon dashboards get mz-mon-env-top -o yaml
```

Add `--share-link` to a query to print it as a Grafana Explore URL that you can
send to someone working in a browser.

`gcx` switches to JSON output when an AI agent such as Claude Code or Cursor
runs it. It also bundles agent skills that teach an agent its commands:

```bash
gcx agent skills install setup-gcx debug-with-grafana manage-dashboards
```

## See also

- [Grafana](/observability/self-managed/grafana/), for enabling and reaching
  the bundled Grafana.
- [How logs and metrics are stored](/observability/self-managed/storage/), for
  where the data lives and how long it is kept.
- [Observability tools
  ⧉](https://materializeinc.github.io/materialize-monitoring/o11y-tools/overview/)
  in the monitoring stack's documentation.
