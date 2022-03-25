---
title: "Cloud Deployments"
description: "View and manage your Materialize Cloud deployments."
disable_toc: true
menu:
  main:
    parent: "cloud"
---

{{< cloud-notice >}}

The [Deployments page](http://cloud.materialize.com/deployments) is your Materialize Cloud dashboard. You can use it to:

- View a deployment's details by clicking on the deployment name.
- [Change a deployment's name or size](../change-deployment-details)
- [Create new deployments](../create-deployments)
- [Delete deployments](../destroy-deployments)
- [Update deployments](../maintenance-windows/#how-to-upgrade)
- [View logs and troubleshoot](../troubleshoot-cloud)

## Details

The Details card shows you basic information about your deployment:

- Name
- Status
- Hostname
- Static IP address
- Materialize version
- Size
- Cloud provider and region
- Cluster ID

You can change the deployment name or size by clicking **Edit**.

## Connect

The Connect card lets you download the TLS certificates you need for [connection](../connect-to-materialize-cloud). It also displays customized connection strings for psql and a customized configuration instructions for some common third-party monitoring tools.

## Integrations

The Integrations card enables or disables supported integrations. Right now, that's just the Tailscale VPN for secure [connections](../connect-to-materialize-cloud), but we're working on an integration with Datadog for analytics.

## Metrics

The Metrics card shows memory and CPU utilization for different ranges of time, and offers access to logs. You can use these to determine whether to [change your deployment size](../change-deployment-details), or to [troubleshoot problems](../troubleshoot-cloud).
