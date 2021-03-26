---
title: "Materialize Cloud Account Limits"
description: "Learn about Materialize Cloud "
menu:
  main:
    parent: "cloud"
    weight: 3
---

{{< cloud-notice >}}

For the most part, Materialize Cloud offers the same functionality as the installed version. The major exceptions are:

* Materialize Cloud doesn't support using files as data sources, only streaming sources like Kafka.
* By default, Materialize Cloud only permits two deployments. If you need more than that, <a href="mailto:support@materialize.com">let us know</a>.
* We reserve the right to terminate your instance. This may happen after prolonged inactivity or if we need to upgrade Materialize Cloud. Catalog items persist across sessions.
* Right now, we only support one user per account. If you'd like multiple users to access your Cloud console, you'll need to share the login. Alternatively, any user can connect to a Cloud deployment as you share your psql connection string. <!-- Is this correct? -->

## Related topics

* [Get Started with Materialize Cloud](../get-started-with-materialize-cloud)
