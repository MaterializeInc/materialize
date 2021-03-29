---
title: "Materialize Cloud Management"
description: "Learn how to perform version upgrades and create and destroy deployments."
menu:
  main:
    parent: "cloud"
    weight: 6
---

{{< cloud-notice >}}

You can perform all Materialize Cloud management function from the [main screen](https://cloud.materialize.com/deployments) of your Materialize Cloud account:

* Creating new deployments
* Viewing deployment details
* Deleting deployments

You can also use this screen to obtain TLS CA certificates and connect to Materialize Cloud. See [Connect to Materialize Cloud](../connect-to-materialize-cloud) for more details.

# Create a deployment

To create a deployment, click **Create deployment**.

The deployment is automatically created and assigned a name and a hostname. This should only take about a minute and a half.

The free version of Materialize Cloud allows two deployments. If you need more, please [contact us](../support).

# Destroy a deployment
<!-- I would still like to replace "destroy" with "delete". -->

Destroying a deployment destroys all data and catalog items. They cannot be restored.

To destroy a deployment, click **Destroy deployment**, enter the confirmation phrase, and click **Yes, destroy my deployment**.

# Upgrade Materialize Cloud

Upgrading Materialize Cloud requires a brief downtime. For this reason, we don't automatically upgrade the Cloud version of your deployments. An **Upgrade** button appears on the Cloud management screen when a new version is available. You can apply the upgrade at the time that works best for you.

If the upgrade is unsuccessful, you will see a **Downgrade** button. Click this to restore the previous Materialize version.

## Related topics

* [Connect to Materialize](../connect-to-materialize-cloud)
* [Materialize Cloud Account Limits](../account-limits)
