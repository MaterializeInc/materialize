<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Ingest data](/docs/ingest-data/)
 /  [Redpanda](/docs/ingest-data/redpanda/)

</div>

# Redpanda Cloud

This guide goes through the required steps to connect Materialize to a
Redpanda Cloud cluster. If you already have a Redpanda Cloud cluster up
and running, skip straight to [Step 2](#step-2-create-a-user).

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

## Step 1. Create a Redpanda Cloud cluster

<div class="note">

**NOTE:**

Once created, provisioning a Dedicated Redpanda Cloud cluster can take
up to 40 minutes.

Serverless clusters are provisioned in a few minutes.

</div>

1.  Sign in to the [Redpanda Cloud
    console](https://cloud.redpanda.com/).

2.  Create a new namespace or select an existing one where you want to
    create the cluster.

3.  Choose the type of cluster you want to create.

4.  Enter a cluster name, and specify the rest of the settings based on
    your needs.

5.  Click **Next** and select the network settings for your cluster.

6.  Next, click **Create** to create the cluster.

## Step 2. Create a user

1.  Navigate to the [Redpanda Cloud
    console](https://cloud.redpanda.com/).

2.  Choose the Redpanda cluster you created in **Step 1**.

3.  Click on the **Security** tab.

4.  In the **Security** section, choose **Create User**.

5.  Specify a name and a password along with the SASL Mechanism for the
    user and click **Create**.

Take note of the user you just created, as well as the password and the
SASL Mechanism you chose; you’ll need them later on. Keep in mind that
the password contains sensitive information, and you should store it
somewhere safe!

## Step 3. Create an Access Control List

1.  Next, still in the **Security** section, click on the **ACL** tab.

2.  Click **Add ACL**,

3.  Choose the **User** you just created.

4.  Choose the **Read** and **Write** permissions for the **Topics**
    section.

5.  You can grant access on a per-topic basis, but you can also choose
    to grant access to all topics by adding a `*` in the **Topics ID**
    input field.

6.  Click **Create**.

## Step 4. Create a topic

To start using Materialize with Redpanda, you need to point it to an
existing Redpanda topic you want to read data from. If the topic you
want to ingest already exists, you can skip this step.

Otherwise, you can install Redpanda Keeper (aka `rpk`) on the client
machine from the previous step to create a topic. For guidance on how to
use `rpk`, check the [Redpanda
documentation](https://docs.redpanda.com/docs/reference/rpk-commands/#rpk-topic-create).

## Step 5. Create a connection

Now that you’ve configured your Redpanda cluster, you can start
ingesting data into Materialize. The exact steps depend on your
networking configuration, so start by selecting the relevant option.

<div class="code-tabs">

<div class="tab-content">

<div id="tab-public-cluster" class="tab-pane" title="Public cluster">

1.  Open the [Redpanda Cloud console](https://cloud.redpanda.com/) and
    select your cluster.

2.  Click on **Overview**

3.  Copy the URL under **Cluster hosts**. This will be your
    `<redpanda-broker-url>` going forward.

4.  In the Materialize [SQL shell](/docs/console/), or your preferred
    SQL client, create a connection with your Redpanda Cloud cluster
    access and authentication details using the
    [`CREATE CONNECTION`](/docs/sql/create-connection/) command:

    <div class="highlight">

    ``` chroma
      -- The credentials of your Redpanda Cloud user.
      CREATE SECRET redpanda_username AS '<your-username>';
      CREATE SECRET redpanda_password AS '<your-password>';

      CREATE CONNECTION rp_connection TO KAFKA (
          BROKER '<redpanda-broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-256', -- The SASL mechanism you chose in Step 2.
          SASL USERNAME = SECRET redpanda_username,
          SASL PASSWORD = SECRET redpanda_password
      );
    ```

    </div>

</div>

<div id="tab-use-aws-privatelink-cloud-only" class="tab-pane"
title="Use AWS PrivateLink (Cloud-only)">

[AWS PrivateLink](https://aws.amazon.com/privatelink/) lets you connect
Materialize to your Redpanda Cloud instance without exposing traffic to
the public internet. To use AWS PrivateLink, your Redpanda cluster must
be deployed in a region supported by Materialize:
`us-east-1`,`us-west-2`, or `eu-west-1`.

1.  **Enable PrivateLink in your Redpanda Cloud cluster.**

    Alter your cluster to enable PrivateLink with an empty
    `allowed_principals` field using the template script below. Make
    sure to record the PrivateLink service name.

    **Note:** This can take up to 15 minutes. You can re-run the script
    (or just the `GET` request) periodically until it resolves.

    <div class="highlight">

    ``` chroma
    PUBLIC_API_ENDPOINT="https://api.cloud.redpanda.com"
    REGION=US-EAST-1
    CLOUD_CLIENT_ID="<your-redpanda-client-id>"
    CLOUD_CLIENT_SECRET="<your-redpanda-client-secret>"
    CLUSTER_ID="<your-redpanda-cluster-id>"

    AUTH_TOKEN=$(
      curl -s -X POST 'https://auth.prd.cloud.redpanda.com/oauth/token' \
             -H 'content-type: application/x-www-form-urlencoded' \
             -d grant_type=client_credentials \
             -d client_id=$CLOUD_CLIENT_ID \
             -d client_secret=$CLOUD_CLIENT_SECRET \
             -d audience=cloudv2-production.redpanda.cloud | jq .access_token | sed 's/"//g'
    )

    CLUSTER_PATCH_BODY="$(cat <<EOF
     {
       "aws_private_link": {
         "enabled": true,
         "allowed_principals": [ ]
       }
     }
    EOF
    )"

    curl -X PATCH \
       -H "Content-Type: application/json" \
       -H "Authorization: Bearer $AUTH_TOKEN" \
       -d "$CLUSTER_PATCH_BODY" \
        $PUBLIC_API_ENDPOINT/v1beta2/clusters/$CLUSTER_ID

    curl -X GET \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $AUTH_TOKEN" \
        $PUBLIC_API_ENDPOINT/v1beta2/clusters/$CLUSTER_ID | jq \
        '.cluster.aws_private_link'
    ```

    </div>

2.  In the Materialize [SQL shell](/docs/console/), or your preferred
    SQL client, create a [PrivateLink
    connection](/docs/ingest-data/network-security/privatelink/) using
    the service name from the previous step. Be sure to specify **all
    availability zones** of your Redpanda Cloud cluster.

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION rp_privatelink TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-abcdefghijk',
      AVAILABILITY ZONES ('use1-az4','use1-az1','use1-az2')
    );
    ```

    </div>

3.  Retrieve the AWS principal for the AWS PrivateLink connection you
    just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'rp_privatelink';
    ```

    </div>

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

4.  Patch your Redpanda Cloud cluster to accept connections from the AWS
    principal:

    <div class="highlight">

    ``` chroma
    PUBLIC_API_ENDPOINT="https://api.cloud.redpanda.com"
    REGION=US-EAST-1
    CLOUD_CLIENT_ID="<your-redpanda-client-id>"
    CLOUD_CLIENT_SECRET="<your-redpanda-client-secret>"
    CLUSTER_ID="<your-redpanda-cluster-id>"

    MATERIALIZE_CONNECTION_ARN="<arn-created-for-materialize-aws-privatelink-connection>"

    AUTH_TOKEN=$(
    curl -s -X POST 'https://auth.prd.cloud.redpanda.com/oauth/token' \
           -H 'content-type: application/x-www-form-urlencoded' \
           -d grant_type=client_credentials \
           -d client_id=$CLOUD_CLIENT_ID \
           -d client_secret=$CLOUD_CLIENT_SECRET \
           -d audience=cloudv2-production.redpanda.cloud | jq .access_token | sed 's/"//g'
    )

    CLUSTER_PATCH_BODY="$(cat <<EOF
     {
       "aws_private_link": {
         "enabled": true,
         "allowed_principals": ["$MATERIALIZE_CONNECTION_ARN"]
       }
     }
    EOF
    )"

    curl -X PATCH \
       -H "Content-Type: application/json" \
       -H "Authorization: Bearer $AUTH_TOKEN" \
       -d "$CLUSTER_PATCH_BODY" \
       $PUBLIC_API_ENDPOINT/v1beta2/clusters/$CLUSTER_ID
    ```

    </div>

5.  In Materialize, validate the AWS PrivateLink connection you created
    using the [`VALIDATE CONNECTION`](/docs/sql/validate-connection)
    command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION rp_privatelink;
    ```

    </div>

    If no validation error is returned, move to the next step.

    This can take several minutes, and report errors like
    `Error: Endpoint cannot be discovered` while the policies are being
    updated and the endpoint resources are being re-evaluated. If the
    validation errors persist for longer than 10 minutes, double-check
    the ARNs and service names and [contact our team](/docs/support/).

6.  Finally, create a connection to your Redpanda Cloud cluster using
    the AWS Privatelink connection you created earlier:

    <div class="highlight">

    ``` chroma
    -- The credentials of your Redpanda Cloud user.
    CREATE SECRET redpanda_username AS '<your-username>';
    CREATE SECRET redpanda_password AS '<your-password>';

    CREATE CONNECTION rp_connection TO KAFKA (
        AWS PRIVATELINK rp_privatelink (PORT 30292)
        SASL MECHANISMS = 'SCRAM-SHA-256', -- The SASL mechanism you chose in Step 2.
        SASL USERNAME = SECRET redpanda_username,
        SASL PASSWORD = SECRET redpanda_password
    );
    ```

    </div>

</div>

</div>

</div>

## Step 6. Start ingesting data

Once you have created the connection, you can use the connection in the
[`CREATE SOURCE`](/docs/sql/create-source/) command to your Redpanda
Cloud cluster and start ingesting data from your target topic. By
default, the source will be created in the active cluster; to use a
different cluster, use the `IN CLUSTER` clause.

<div class="highlight">

``` chroma
CREATE SOURCE rp_source
  -- The topic you want to read from.
  FROM KAFKA CONNECTION redpanda_cloud (TOPIC '<topic-name>')
  FORMAT JSON;
```

</div>

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/redpanda/redpanda-cloud.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
