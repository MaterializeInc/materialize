---
title: "Materialize Cloud Quickstart"
description: ""
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

1. Go to [https://cloud.materialize.com](https://cloud.materialize.com#signup) and enter an email and password.
2. Enter the access code that was sent to your email address.

## Create and connect to a Materialize Cloud deployment

Once you [sign up](https://cloud.materialize.com/signup) for Materialize Cloud and [log in](https://cloud.materialize.com), you use the main screen to create, upgrade, or delete deployments and to obtain the TLS CA you need to connect to the Cloud from a terminal.

By default, you can create up to two deployments. If you're interested in more, [let us know](../get-help-for-materialize-cloud).

1. Click **Create deployment**. A deployment is created with an assigned name and hostname.
2. Click **Connect**.
3. Click **Download certificates**.
4. Unzip the certificate package.
5. To connect to the deployment with the psql command-line tool, copy the command from the connection dialog and run it from the directory which contains the certificates.

  Sample command:

  ```
  psql "postgresql://materialize@hostname:6875/materialize?sslcert=materialize.crt&sslkey=materialize.key&sslrootcert=ca.crt"
  ```

If you've just created the deployment, you may need to wait a minute or two before you'll be able to connect.

## Connect to a data source

petros setting up pubnub feed (simple source)
materialize Kafka
public kafka endpoint issue 65
cloud issue 92
wikipedia demo feed

CREATE SOURCE pubnub ?

## Run a sample query

## Related topics

* [Connect to Materialize Cloud](../connect-to-materialize-cloud)
* [Materialize Cloud Account Limits](../materialize-cloud-account limits)
