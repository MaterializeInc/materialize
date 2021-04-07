---
title: "Materialize Cloud Account Limits"
description: "Learn Materialize Cloud's account limits and its differences from the installed version."
menu:
  main:
    parent: "cloud"
    weight: 3
---

{{< cloud-notice >}}

For the most part, Materialize Cloud offers the same functionality as the installed version. The major exceptions are:

* Materialize Cloud doesn't support using files as data sources, only streaming sources like Kafka.
* By default, Materialize Cloud only permits two deployments. If you need more than that, [let us know](../support).
* We reserve the right to terminate a session on your deployment. This may happen after prolonged inactivity or if we need to upgrade Materialize Cloud. Catalog items persist across sessions.
* Right now, we only support one user per account. If you'd like multiple users to access your Cloud console, you'll need to share the login. Alternatively, any user can connect to a Cloud deployment as long as you share your TLS certificates and your psql connection string.

## Related topics

* [What Is Materialize?](/overview/what-is-materialize)
* [Connect to Materialize Cloud](../connect-to-materialize-cloud)
