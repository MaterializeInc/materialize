---
title: "Materialize Cloud Quickstart"
description: "Set up a Materialize Cloud account, create deployments, and connect to data sources."
menu:
  main:
    parent: "cloud"
    weight: 2
---

{{< cloud-notice >}}


To get you started, we'll walk you through the following:

1. Signing up for Materialize Cloud.
2. Creating a Materialize Cloud deployment.
3. Connecting to the deployments from a terminal.
4. Connecting to a data source.
5. Running sample queries.

## Sign up for Materialize Cloud

Sign up at [https://cloud.materialize.com](https://cloud.materialize.com#signup).

## Create and connect to a Materialize Cloud deployment

Once you [sign up](https://cloud.materialize.com/signup) for Materialize Cloud and [log in](https://cloud.materialize.com), you use the Cloud management screen to create, upgrade, or destroy deployments, and to obtain the TLS CA you need to install on your local machine.

By default, you can create up to two deployments. If you're interested in more, [let us know](../support).

1. Click **Create deployment**. A deployment is created with an assigned name and hostname.
2. Click **Connect**.
3. Click **Download certificates**.
4. Unzip the certificate package.
5. To connect to the deployment with the psql command-line tool, copy the command from the connection dialog and run it from the directory which contains the certificates. (The sample code below will not work unless you substitute your real hostname.)

      ```
      psql "postgresql://materialize@hostname:6875/materialize?sslcert=materialize.crt&sslkey=materialize.key&sslrootcert=ca.crt"
      ```

    If you've just created the deployment, you may need to
wait a minute or two before you'll be able to connect.

## Connect to a data source

<!-- I couldn't figure out this for PubNub, but may not have been looking in the right place/using the right search terms. Can you provide an example or a link to the right PubNub docs? -->

If you have your own streaming data source set up, you can use that instead. See [`CREATE SOURCE`](../../sql/create-source) for details. Note that the `CREATE SOURCE` documentation for offline file sources does not apply to Materialize Cloud, which only supports streaming sources.

## Create a view and run a sample query

<!-- To be added once I can connect to PubNub-->

## Related topics

* [Connect to Materialize Cloud](../connect-to-materialize-cloud)
* [Materialize Cloud Account Limits](../account-limits)
* [Materialize Overview](../../overview/architecture)
* [`CREATE SOURCE`](../../sql/create-source)
