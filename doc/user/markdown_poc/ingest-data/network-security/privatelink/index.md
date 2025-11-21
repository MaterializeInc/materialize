<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Ingest data](/docs/ingest-data/)

</div>

# AWS PrivateLink connections (Cloud-only)

Materialize can connect to a Kafka broker, a Confluent Schema Registry
server, a PostgreSQL database, or a MySQL database through an [AWS
PrivateLink](https://aws.amazon.com/privatelink/) service.

In this guide, we’ll cover how to create `AWS PRIVATELINK` connections
and retrieve the AWS principal needed to configure the AWS PrivateLink
service.

## Create an AWS PrivateLink connection

<div class="code-tabs">

<div class="tab-content">

<div id="tab-kafka-on-aws" class="tab-pane" title="Kafka on AWS">

<div class="note">

**NOTE:** Materialize provides a Terraform module that automates the
creation and configuration of AWS resources for a PrivateLink
connection. For more details, see the Terraform module repositories for
[Amazon
MSK](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
and [self-managed Kafka
clusters](https://github.com/MaterializeInc/terraform-aws-kafka-privatelink).

</div>

This section covers how to create AWS PrivateLink connections and
retrieve the AWS principal needed to configure the AWS PrivateLink
service.

1.  Create target groups. Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    **for each broker** with the following details:

    a\. Target type as **IP address**.

    b\. Protocol as **TCP**.

    c\. Port as **9092**, or the port that you are using in case it is
    not 9092 (e.g. 9094 for TLS or 9096 for SASL).

    d\. Make sure that the target group is in the same VPC as the Kafka
    cluster.

    e\. Click next, and register the respective Kafka broker to each
    target group using its IP address.

2.  Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the Kafka brokers are
    in.

3.  Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for every Kafka broker that forwards to the corresponding target
    group you created (e.g. `b-1`, `b-2`, `b-3`).

    The listener port needs to be unique, and will be used later on in
    the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a\. Port `9001` → broker `b-1...`.

    b\. Port `9002` → broker `b-2...`.

    c\. Port `9003` → broker `b-3...`.

4.  Verify security groups and health checks. Once the TCP listeners
    have been created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    for each target group are passing and that the targets are reported
    as healthy.

    If you have set up a security group for your Kafka cluster, you must
    ensure that it allows traffic on both the listener port and the
    health check port.

    **Remarks**:

    a\. Network Load Balancers do not have associated security groups.
    Therefore, the security groups for your targets must use IP
    addresses to allow traffic.

    b\. You can’t use the security groups for the clients as a source in
    the security groups for the targets. Therefore, the security groups
    for your targets must use the IP addresses of the clients to allow
    traffic. For more details, check the [AWS
    documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

5.  Create a VPC [endpoint
    service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint
    service.

6.  Create an AWS PrivateLink connection. In Materialize, create an [AWS
    PrivateLink
    connection](/docs/sql/create-connection/#aws-privatelink) that
    references the endpoint service that you created in the previous
    step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same
    region** as your Materialize environment:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
      AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - Replace the `AVAILABILITY ZONES` list with the IDs of the
      availability zones in your AWS account. For in-region connections
      the availability zones of the NLB and the consumer VPC **must
      match**.

      To find your availability zone IDs, select your database in the
      RDS Console and click the subnets under **Connectivity &
      security**. For each subnet, look for **Availability Zone ID**
      (e.g., `use1-az6`), not **Availability Zone** (e.g.,
      `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different
    region** to the one where your Materialize environment is deployed:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
      -- For now, the AVAILABILITY ZONES clause **is** required, but will be
      -- made optional in a future release.
      AVAILABILITY ZONES ()
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - The service name region refers to where the endpoint service was
      created. You **do not need** to specify `AVAILABILITY ZONES`
      manually — these will be optimally auto-assigned when none are
      provided.

    - For Kafka connections, it is required for [cross-zone load
      balancing](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html)
      to be enabled on the VPC endpoint service’s NLB when using
      cross-region Privatelink.

7.  Configure the AWS PrivateLink service. Retrieve the AWS principal
    for the AWS PrivateLink connection you just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    </div>

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from
    the provided AWS principal.

8.  If your AWS PrivateLink service is configured to require acceptance
    of connection requests, you must manually approve the connection
    request from Materialize after executing `CREATE CONNECTION`. For
    more details, check the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service
    connection to show up, so you would need to wait for the endpoint
    service connection to be ready before you create a source.

9.  Validate the AWS PrivateLink connection you created using the
    [`VALIDATE CONNECTION`](/docs/sql/validate-connection) command:

    <div class="highlight">

    ``` chroma
    VALIDATE CONNECTION privatelink_svc;
    ```

    </div>

    If no validation error is returned, move to the next step.

10. Create a source connection

    In Materialize, create a source connection that uses the AWS
    PrivateLink connection you just configured:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKERS (
            -- The port **must exactly match** the port assigned to the broker in
            -- the TCP listerner of the NLB.
            'b-1.hostname-1:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9001, AVAILABILITY ZONE 'use1-az2'),
            'b-2.hostname-2:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9002, AVAILABILITY ZONE 'use1-az1'),
            'b-3.hostname-3:9096' USING AWS PRIVATELINK privatelink_svc (PORT  9003, AVAILABILITY ZONE 'use1-az4')
        ),
        -- Authentication details
        -- Depending on the authentication method the Kafka cluster is using
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = 'foo',
        SASL PASSWORD = SECRET kafka_password
    );
    ```

    </div>

    If you run into connectivity issues during source creation, make
    sure that:

    - The `(PORT <port_number>)` value **exactly matches** the port
      assigned to the corresponding broker in the **TCP listener** of
      the Network Load Balancer. Misalignment between ports and broker
      addresses is the most common cause for connectivity issues.

    - For **in-region connections**, the correct availability zone is
      specified for each broker.

</div>

<div id="tab-postgresql-on-aws" class="tab-pane"
title="PostgreSQL on AWS">

<div class="note">

**NOTE:** Materialize provides a Terraform module that automates the
creation and configuration of AWS resources for a PrivateLink
connection. For more details, see the [Terraform module
repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).

</div>

1.  #### Create target groups

    Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your RDS or Aurora or Aurora instance with the following
    details:

    a\. Target type as **IP address**.

    b\. Protocol as **TCP**.

    c\. Port as **5432**, or the port that you are using in case it is
    not 5432.

    d\. Make sure that the target group is in the same VPC as the RDS or
    Aurora instance.

    e\. Click next, and register the respective RDS or Aurora instance
    to the target group using its IP address.

2.  #### Verify security groups and health checks

    Once the target groups have been created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    are passing and that the targets are reported as healthy.

    If you have set up a security group for your RDS or Aurora instance,
    you must ensure that it allows traffic on the health check port.

    **Remarks**:

    a\. Network Load Balancers do not have associated security groups.
    Therefore, the security groups for your targets must use IP
    addresses to allow traffic.

    b\. You can’t use the security groups for the clients as a source in
    the security groups for the targets. Therefore, the security groups
    for your targets must use the IP addresses of the clients to allow
    traffic. For more details, check the [AWS
    documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

3.  #### Create a Network Load Balancer (NLB)

    Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the RDS or Aurora
    instance is in.

4.  #### Create TCP listeners

    Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your RDS or Aurora instance that forwards to the corresponding
    target group you created.

5.  #### Create a VPC endpoint service

    Create a VPC [endpoint
    service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint
    service.

6.  #### Create an AWS PrivateLink connection

    In Materialize, create an [AWS PrivateLink
    connection](/docs/sql/create-connection/#aws-privatelink) that
    references the endpoint service that you created in the previous
    step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same
    region** as your Materialize environment:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
      AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - Replace the `AVAILABILITY ZONES` list with the IDs of the
      availability zones in your AWS account. For in-region connections
      the availability zones of the NLB and the consumer VPC **must
      match**.

      To find your availability zone IDs, select your database in the
      RDS Console and click the subnets under **Connectivity &
      security**. For each subnet, look for **Availability Zone ID**
      (e.g., `use1-az6`), not **Availability Zone** (e.g.,
      `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different
    region** to the one where your Materialize environment is deployed:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
      -- For now, the AVAILABILITY ZONES clause **is** required, but will be
      -- made optional in a future release.
      AVAILABILITY ZONES ()
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - The service name region refers to where the endpoint service was
      created. You **do not need** to specify `AVAILABILITY ZONES`
      manually — these will be optimally auto-assigned when none are
      provided.

## Configure the AWS PrivateLink service

1.  Retrieve the AWS principal for the AWS PrivateLink connection you
    just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    </div>

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from
    the provided AWS principal.

2.  If your AWS PrivateLink service is configured to require acceptance
    of connection requests, you must manually approve the connection
    request from Materialize after executing `CREATE CONNECTION`. For
    more details, check the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service
    connection to show up, so you would need to wait for the endpoint
    service connection to be ready before you create a source.

## Validate the AWS PrivateLink connection

Validate the AWS PrivateLink connection you created using the
[`VALIDATE CONNECTION`](/docs/sql/validate-connection) command:

<div class="highlight">

``` chroma
VALIDATE CONNECTION privatelink_svc;
```

</div>

If no validation error is returned, move to the next step.

## Create a source connection

In Materialize, create a source connection that uses the AWS PrivateLink
connection you just configured:

<div class="highlight">

``` chroma
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc
);
```

</div>

This PostgreSQL connection can then be reused across multiple
[`CREATE SOURCE`](https://materialize.com/docs/sql/create-source/postgres/)
statements.

</div>

<div id="tab-mysql-on-aws" class="tab-pane" title="MySQL on AWS">

<div class="note">

**NOTE:** Materialize provides a Terraform module that automates the
creation and configuration of AWS resources for a PrivateLink
connection. For more details, see the [Terraform module
repository](https://github.com/MaterializeInc/terraform-aws-rds-privatelink).

</div>

1.  #### Create target groups

    Create a dedicated [target
    group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html)
    for your RDS instance with the following details:

    a\. Target type as **IP address**.

    b\. Protocol as **TCP**.

    c\. Port as **3306**, or the port that you are using in case it is
    not 3306.

    d\. Make sure that the target group is in the same VPC as the RDS
    instance.

    e\. Click next, and register the respective RDS instance to the
    target group using its IP address.

2.  #### Verify security groups and health checks

    Once the target groups have been created, make sure that the [health
    checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html)
    are passing and that the targets are reported as healthy.

    If you have set up a security group for your RDS instance, you must
    ensure that it allows traffic on the health check port.

    **Remarks**:

    a\. Network Load Balancers do not have associated security groups.
    Therefore, the security groups for your targets must use IP
    addresses to allow traffic.

    b\. You can’t use the security groups for the clients as a source in
    the security groups for the targets. Therefore, the security groups
    for your targets must use the IP addresses of the clients to allow
    traffic. For more details, check the [AWS
    documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

3.  #### Create a Network Load Balancer (NLB)

    Create a [Network Load
    Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html)
    that is **enabled for the same subnets** that the RDS instance is
    in.

4.  #### Create TCP listeners

    Create a [TCP
    listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html)
    for your RDS instance that forwards to the corresponding target
    group you created.

5.  #### Create a VPC endpoint service

    Create a VPC [endpoint
    service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html)
    and associate it with the **Network Load Balancer** that you’ve just
    created.

    Note the **service name** that is generated for the endpoint
    service.

6.  #### Create an AWS PrivateLink connection

    In Materialize, create an [AWS PrivateLink
    connection](/docs/sql/create-connection/#aws-privatelink) that
    references the endpoint service that you created in the previous
    step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same
    region** as your Materialize environment:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
      AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - Replace the `AVAILABILITY ZONES` list with the IDs of the
      availability zones in your AWS account. For in-region connections
      the availability zones of the NLB and the consumer VPC **must
      match**.

      To find your availability zone IDs, select your database in the
      RDS Console and click the subnets under **Connectivity &
      security**. For each subnet, look for **Availability Zone ID**
      (e.g., `use1-az6`), not **Availability Zone** (e.g.,
      `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different
    region** to the one where your Materialize environment is deployed:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
      SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
      -- For now, the AVAILABILITY ZONES clause **is** required, but will be
      -- made optional in a future release.
      AVAILABILITY ZONES ()
    );
    ```

    </div>

    - Replace the `SERVICE NAME` value with the service name you noted
      earlier.

    - The service name region refers to where the endpoint service was
      created. You **do not need** to specify `AVAILABILITY ZONES`
      manually — these will be optimally auto-assigned when none are
      provided.

## Configure the AWS PrivateLink service

1.  Retrieve the AWS principal for the AWS PrivateLink connection you
    just created:

    <div class="highlight">

    ``` chroma
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    </div>

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from
    the provided AWS principal.

2.  If your AWS PrivateLink service is configured to require acceptance
    of connection requests, you must manually approve the connection
    request from Materialize after executing `CREATE CONNECTION`. For
    more details, check the [AWS PrivateLink
    documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service
    connection to show up, so you would need to wait for the endpoint
    service connection to be ready before you create a source.

## Validate the AWS PrivateLink connection

Validate the AWS PrivateLink connection you created using the
[`VALIDATE CONNECTION`](/docs/sql/validate-connection) command:

<div class="highlight">

``` chroma
VALIDATE CONNECTION privatelink_svc;
```

</div>

If no validation error is returned, move to the next step.

## Create a source connection

In Materialize, create a source connection that uses the AWS PrivateLink
connection you just configured:

<div class="highlight">

``` chroma
CREATE CONNECTION mysql_connection TO MYSQL (
      HOST <host>,
      PORT 3306,
      USER 'materialize',
      PASSWORD SECRET mysqlpass,
      SSL MODE REQUIRED,
      AWS PRIVATELINK privatelink_svc
);
```

</div>

This MySQL connection can then be reused across multiple
[`CREATE SOURCE`](https://materialize.com/docs/sql/create-source/mysql/)
statements.

</div>

</div>

</div>

## Related pages

- [`CREATE SECRET`](/docs/sql/create-secret)
- [`CREATE CONNECTION`](/docs/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/docs/sql/create-source/kafka)
- Integration guides: [Self-hosted
  PostgreSQL](/docs/ingest-data/postgres/self-hosted/), [Amazon RDS for
  PostgreSQL](/docs/ingest-data/postgres/amazon-rds/), [Self-hosted
  Kafka](/docs/ingest-data/kafka/kafka-self-hosted), [Amazon
  MSK](/docs/ingest-data/kafka/amazon-msk), [Redpanda
  Cloud](/docs/ingest-data/redpanda/redpanda-cloud/)

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/network-security/privatelink.md"
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
