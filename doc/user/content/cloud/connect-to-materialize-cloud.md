---
title: "Connect to Materialize Cloud"
description: "You can connect Materialize Cloud to streaming data sources."
menu:
  main:
    parent: "cloud"
    weight: 4
---

{{< cloud-notice >}}

## Connect to  Materialize Cloud

Once you've [created a deployment](../cloud-management/#create-a-deployment), you'll need to install TLS certificates on your local machine to connect.

2. On the [Cloud Management page](https://cloud.materialize.com/deployments), click **Connect**.
3. Click **Download certificates**.
4. Unzip the certificate package.
5. To connect to the deployment with the psql command-line tool, copy the command from the connection dialog and run it from the directory which contains the certificates. (The sample code below will not work unless you substitute your real hostname.)

      ```
      psql "postgresql://materialize@hostname:6875/materialize?sslcert=materialize.crt&sslkey=materialize.key&sslrootcert=ca.crt"
      ```

    If you've just created the deployment, you may need to
wait a minute or two before you'll be able to connect.

## Related topics

* [Cloud management](../cloud-management)
