---
title: "Connect to Materialize Cloud"
description: "You can connect Materialize Cloud to streaming data sources."
menu:
  main:
    parent: "cloud"
    weight: 4
---

{{< cloud-notice >}}

Once you've created a deployment, you'll need to install TLS certificates on your local machine to connect. You can obtain these on the [Deployments](https://cloud.materialize.com/deployments) page.

{{% cloud-connection-details %}}

**Note:** Right now, we only support one user per account. If you'd like multiple users to access your Cloud console, you'll need to share the login. Alternatively, any user can connect to a Cloud deployment as long as you share your TLS certificates and your `psql` connection string.

## Related topics

* [Create deployments](../create-deployments)
* [`CREATE SOURCE`](/sql/create-source)
