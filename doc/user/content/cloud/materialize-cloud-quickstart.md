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

Sign up at [https://cloud.materialize.com](https://cloud.materialize.com#signup).

## Create and connect to a Materialize Cloud deployment

Once you [sign up](https://cloud.materialize.com/signup) for Materialize Cloud and [log in](https://cloud.materialize.com), you use the main screen to create, upgrade, or delete deployments and to obtain the TLS CA you need to connect to the Cloud from a terminal.

By default, you can create up to two deployments. If you're interested in more, [let us know](../get-help-for-materialize-cloud).

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

For this quickstart, we use the public Materialize Kafka stream topic user_behavior for a materialized source:

```
CREATE MATERIALIZED SOURCE "materialize"."public"."src_user_behavior" FROM KAFKA BROKER '192.168.1.254:9092' TOPIC 'user_behavior' FORMAT BYTES ;
SHOW SOURCES;
```

If you have your streaming data source, you can use that instead. See [`CREATE SOURCE`](../../sql/create-source) for details.

## Run a sample query

## Related topics

* [Connect to Materialize Cloud](../connect-to-materialize-cloud)
* [Materialize Cloud Account Limits](../materialize-cloud-account-limits)
* [`CREATE SOURCE`](../../sql/create-source)
