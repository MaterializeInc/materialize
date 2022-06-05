---
title: "Maintenance windows"
description: "Update Materialize Cloud."
menu:
  main:
    parent: "cloud"
    weight:
aliases:
  - /cloud/upgrade-deployments
  - /cloud/update-deployments
---

{{< cloud-notice >}}

The regular maintenance window for Materialize Cloud is **Wednesdays 16:30-18:30 US/Eastern**, during which we will apply software upgrades and make new versions of Cloud available. There may be a forced restart during this time period.

You can sign up on our [status page](https://status.materialize.com) to be notified when new maintenance windows are scheduled and when it's an hour before the window will start.

## How to upgrade

When a new version of Materialize Cloud is available, an **Update available** notice appears on the [Deployments](https://cloud.materialize.com/deployments) page. The update will cause a restart, so be sure to select a time that's good for your business before you click the notice and enter the requested confirmation.

We recommend updating a non-production deployment first and testing to ensure that your materializations and computations continue to work as expected.

If you haven't upgraded a deployment to the latest version within 14 days, we will apply the new version automatically.

## Related topics

* [Create Deployments](../create-deployments)
* [Destroy Deployments](../destroy-deployments)
