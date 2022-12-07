---
title: "AWS MSK - PrivateLink"
description: "How to connect Materialize to an AWS MSK cluster using a Privatelink connection"
menu:
  main:
    parent: "network-security"
    name: "AWS MSK - PrivateLink"
---

Materialize can connect to an AWS MSK cluster over a PrivateLink connection.

In this guide, we'll cover how to:

* Configure an AWS PrivateLink connection
* Create `AWS PRIVATELINK` and `KAFKA` connections in Materialize
* Create an MSK Kafka source that uses the PrivateLink connection

### Prerequisites

Before moving on, double-check that you have:

* A running [AWS MSK Cluster](/integrations/aws-msk)
* A region enabled Materialize

### Steps

1. #### Create Target Groups
    Create [target groups](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html) dedicated for each broker (in this example we will use: `b-1`, `b-2`) with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **9092,** or the port that you are using in case it is not 9092, e.g. 9094 for TLS or 9096 for SASL, etc.

    d. Make sure that the target group is in the same VPC as the MSK cluster.

    e. Click next, and register the respective MSK broker to each target group by its IP address.

    > Make sure to create a separate target group for each broker.

1. #### Security Groups and Health Checks

    Once the target groups have been created, make sure that the [health checks](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-health-checks.html) are passing and that the targets are reported as healthy.

    If you have set up a security group for your MSK cluster, you must ensure that the security groups allow traffic on both the listener port and the health check port.

    **Remarks**:

    a. Network Load Balancers do not have associated security groups. Therefore, the security groups for your targets must use IP addresses to allow traffic.

    b. You can't use the security groups for the clients as a source in the security groups for the targets. Therefore, the security groups for your targets must use the IP addresses of the clients to allow traffic.

    [AWS target groups documentation reference](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/target-group-register-targets.html).

1. #### Create a Network Load Balancer (NLB)
    Create a [Network Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-network-load-balancer.html) that is enabled for the **same subnets that the MSK brokers are in**.

1. #### Create TCP Listeners

    Create a [TCP listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-listener.html) for every MSK broker that forwards to the corresponding target group you created (`b-1`, `b-2`, etc.).

    The listener port needs to be unique, and we will later reference it in the `CREATE CONNECTION` statement.

    For example, you can create a listener for:

    a. Port `9001` → for broker `b-1...`.

    b. Port `9002` → for broker `b-2...`.

    c. Port `9003` → for broker `b-3...` etc.

1. #### VPC Endpoint Service

    Create a VPC [endpoint service](https://docs.aws.amazon.com/vpc/latest/privatelink/create-endpoint-service.html) and associate the **Network Load Balancer** that you’ve just created.

    Note the **service name** that is generated for the endpoint service. You will need it in the next step.

1. #### Create an AWS PrivateLink Connection
     In Materialize, create a [`AWS PRIVATELINK`](/sql/create-connection/#aws-privatelink) connection that references the endpoint service that you created in the previous step.

     ```sql
    CREATE CONNECTION <pl_conn_name> TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az3'),
    );
    ```

    Update the list of the availability zones to match the ones that you are using in your AWS account.

1. #### Allow Access to the VPC Endpoint Service

    After you create the endpoint service, you must [configure the permissions of your AWS PrivateLink service](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html) to accept connections from the connection object's AWS principal. Get the AWS principal from the `mz_aws_privatelink_connections` table.

     ```sql
    SELECT principal
        FROM mz_aws_privatelink_connections plc
        JOIN mz_connections c ON plc.id = c.id
        WHERE c.name = '<pl_conn_name>';
    ```

1. #### Accept the Endpoint Service Connection

    If your AWS PrivateLink service is configured to require acceptance of connection requests, you must manually approve the connection request from Materialize after executing `CREATE CONNECTION`.

    See [accept or reject connection requests](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests) in the AWS PrivateLink documentation.

    It might take some time for the endpoint service connection to show up, so you would need to wait for the endpoint service connection to be ready before you create a source.

1. #### Create Kafka Connection

    In Materialize, create a [`KAFKA`](/sql/create-connection/#kafka) connection object that references the AWS PrivateLink connection that you created in the previous step.

    ```sql
    CREATE CONNECTION <kafka_conn_name> TO KAFKA (
        BROKERS (
            'b-1.hostname-1:9096' USING AWS PRIVATELINK <pl_conn_name> (PORT 9001),
            'b-2.hostname-2:9096' USING AWS PRIVATELINK <pl_conn_name> (PORT 9002),
            'b-3.hostname-3:9096' USING AWS PRIVATELINK <pl_conn_name> (PORT 9003)
        ),
        -- Authentication details
        -- Depending on the authentication method your MSK cluster is using
        SASL MECHANISMS = 'SCRAM-SHA-512',
        SASL USERNAME = SECRET <msk_sasl_username>,
        SASL PASSWORD = SECRET <msk_sasl_password>
    );
    ```

    The `(PORT <port_number>)` value must match the port that you used when creating the **TCP listener** in the Network Load Balancer.

    Change the [Kafka authentication details](/sql/create-connection/#kafka) accordingly based on the authentication method your MSK cluster is using.

1. #### Create a Source

    To create a source, use the [`CREATE SOURCE`](/sql/create-source/) statement and reference the above Kafka connection:

    ```sql
    CREATE SOURCE <source_name>
        FROM KAFKA CONNECTION <kafka_conn_name> (TOPIC '<topic_name>')
        FORMAT TEXT
        WITH (SIZE  'xsmall');
    ```

    Change the [`FORMAT`](/sql/create-source/#formats) based on the format of the data in the topic.

    Adjust the `SIZE` parameter according to the size of your data.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`: Kafka](/sql/create-source/kafka)
- [MSK integration](/integrations/aws-msk)
