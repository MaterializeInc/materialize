---
title: "AWS PrivateLink connections"
description: "How to connect Materialize to a Kafka broker, or a PostgreSQL database using an AWS PrivateLink connection"
menu:
  main:
    parent: "network-security"
    name: "AWS PrivateLink connections"
---

Materialize can connect to a Kafka broker, a Confluent Schema Registry server or
a PostgreSQL database through an [AWS PrivateLink](https://aws.amazon.com/privatelink/)
service. In this guide, we'll cover how to create `AWS PRIVATELINK` connections
and retrieve the AWS principal needed to configure the AWS PrivateLink service.

## Create an AWS PrivateLink connection

{{< tabs tabID="1" >}}
{{< tab "AWS MSK">}}

{{< note >}}
Materialize provides a [Terraform module](https://github.com/MaterializeInc/terraform-aws-msk-privatelink)
that can be used to create the VPC endpoint service (step 5) and the AWS PrivateLink
connection (step 6).
{{< /note >}}

1. #### Create target groups
    Create a dedicated [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html) **for each broker** with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **9092**, or the port that you are using in case it is not 9092 (e.g. 9094 for TLS or 9096 for SASL).

    d. Make sure that the target group is in the same VPC as the MSK cluster.

    e. Click next, and register the respective MSK broker to each target group using its IP address.

1. #### Verify security groups and health checks

    Once the target groups have been created, make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html) are passing and that the targets are reported as healthy.

    If you have set up a security group for your MSK cluster, you must ensure that it allows traffic on both the listener port and the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore, the security groups for your targets must use IP addresses to allow traffic.

    b. You can't use the security groups for the clients as a source in the security groups for the targets. Therefore, the security groups for your targets must use the IP addresses of the clients to allow traffic. For more details, check the [AWS documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. #### Create a Network Load Balancer (NLB)
    Create a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html) that is **enabled for the same subnets** that the MSK brokers are in.

1. #### Create TCP listeners

    Create a [TCP listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html) for every MSK broker that forwards to the corresponding target group you created (e.g. `b-1`, `b-2`, `b-3`).

    The listener port needs to be unique, and will be used later on in the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a. Port `9001` → broker `b-1...`.

    b. Port `9002` → broker `b-2...`.

    c. Port `9003` → broker `b-3...`.

1. #### Create a VPC endpoint service

    Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html) and associate it with the **Network Load Balancer** that you’ve just created.

    Note the **service name** that is generated for the endpoint service.

1. #### Create an AWS PrivateLink Connection
     In Materialize, create a [`AWS PRIVATELINK`](/sql/create-connection/#aws-privatelink) connection that references the endpoint service that you created in the previous step.

     ```sql
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    Update the list of the availability zones to match the ones in your AWS account.

{{< /tab >}}
{{< tab "AWS RDS">}}
1. #### Create target groups
    Create a dedicated [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html) for your RDS instance with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **5432**, or the port that you are using in case it is not 5432.

    d. Make sure that the target group is in the same VPC as the RDS instance.

    e. Click next, and register the respective RDS instance to the target group using its IP address.

1. #### Verify security groups and health checks

    Once the target groups have been created, make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html) are passing and that the targets are reported as healthy.

    If you have set up a security group for your RDS instance, you must ensure that it allows traffic on the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore, the security groups for your targets must use IP addresses to allow traffic.

    b. You can't use the security groups for the clients as a source in the security groups for the targets. Therefore, the security groups for your targets must use the IP addresses of the clients to allow traffic. For more details, check the [AWS documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. #### Create a Network Load Balancer (NLB)
    Create a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html) that is **enabled for the same subnets** that the RDS instance is in.

1. #### Create TCP listeners

    Create a [TCP listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html) for your RDS instance that forwards to the corresponding target group you created.

1. #### Create a VPC endpoint service

    Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html) and associate it with the **Network Load Balancer** that you’ve just created.

    Note the **service name** that is generated for the endpoint service.

1. #### Create an AWS PrivateLink Connection
     In Materialize, create a [`AWS PRIVATELINK`](/sql/create-connection/#aws-privatelink) connection that references the endpoint service that you created in the previous step.

     ```sql
    CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3')
    );
    ```

    Update the list of the availability zones to match the ones that you are using in your AWS account.

{{< /tab >}}
{{< /tabs >}}

## Configure the AWS PrivateLink service

1. Retrieve the AWS principal for the AWS PrivateLink connection you just created:

    ```sql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    ```
       id   |                                 principal
    --------+---------------------------------------------------------------------------
     u1     | arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from the
    provided AWS principal.

1. If your AWS PrivateLink service is configured to require acceptance of connection requests, you must manually approve the connection request from Materialize after executing `CREATE CONNECTION`. For more details, check the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service connection to show up, so you would need to wait for the endpoint service connection to be ready before you create a source.

## Create a source connection

In Materialize, create a source connection that uses the AWS PrivateLink connection you just configured:

{{< tabs tabID="1" >}}
{{< tab "AWS MSK">}}
```sql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'b-1.hostname-1:9096' USING AWS PRIVATELINK privatelink_svc (PORT 9001, AVAILABILITY ZONE 'use1-az2'),
        'b-2.hostname-2:9096' USING AWS PRIVATELINK privatelink_svc (PORT 9002, AVAILABILITY ZONE 'use1-az1'),
        'b-3.hostname-3:9096' USING AWS PRIVATELINK privatelink_svc (PORT 9003, AVAILABILITY ZONE 'use1-az3')
    ),
    -- Authentication details
    -- Depending on the authentication method the MSK cluster is using
    SASL MECHANISMS = 'SCRAM-SHA-512',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```

The `(PORT <port_number>)` value must match the port that you used when creating the **TCP listener** in the Network Load Balancer. Be sure to specify the correct availability zone for each broker.

This Kafka connection can then be reused across multiple [`CREATE SOURCE`](/sql/create-source/kafka/)
statements:

```sql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT BYTES
  WITH (SIZE = '3xsmall');
```
{{< /tab >}}
{{< tab "AWS RDS">}}
```sql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc
);
```

This PostgreSQL connection can then be reused across multiple [CREATE SOURCE](https://materialize.com/docs/sql/create-source/postgres/) statements:

```sql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES
  WITH (SIZE = '3xsmall');
```
{{< /tab >}}
{{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)
- [AWS MSK integration guide](/integrations/aws-msk)
