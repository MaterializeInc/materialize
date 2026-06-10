1. #### Create target groups
    Create a dedicated [target group](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/create-target-group.html) for your RDS instance with the following details:

    a. Target type as **IP address**.

    b. Protocol as **TCP**.

    c. Port as **3306**, or the port that you are using in case it is not 3306.

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

1. #### Create an AWS PrivateLink connection

    In Materialize, create an [AWS PrivateLink connection](/sql/create-connection/#aws-privatelink)
    that references the endpoint service that you created in the previous step.

    ↕️ **In-region connections**

    To connect to an AWS PrivateLink endpoint service in the **same region** as your
    Materialize environment:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.<region_id>.vpce-svc-<endpoint_service_id>',
        AVAILABILITY ZONES ('use1-az1', 'use1-az2', 'use1-az4')
      );
      ```

    - Replace the `SERVICE NAME` value with the service name you noted earlier.

    - Replace the `AVAILABILITY ZONES` list with the IDs of the availability
      zones in your AWS account. For in-region connections the availability
      zones of the NLB and the consumer VPC **must match**.

      To find your availability zone IDs, select your database in the RDS
      Console and click the subnets under **Connectivity & security**. For each
      subnet, look for **Availability Zone ID** (e.g., `use1-az6`),
      not **Availability Zone** (e.g., `us-east-1d`).

    ↔️ **Cross-region connections**

    To connect to an AWS PrivateLink endpoint service in a **different region** to
    the one where your Materialize environment is deployed:

      ```mzsql
      CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
        SERVICE NAME 'com.amazonaws.vpce.us-west-1.vpce-svc-<endpoint_service_id>',
        -- For now, the AVAILABILITY ZONES clause **is** required, but will be
        -- made optional in a future release.
        AVAILABILITY ZONES ()
      );
      ```

    - Replace the `SERVICE NAME` value with the service name you noted earlier.

    - The service name region refers to where the endpoint service was created.
      You **do not need** to specify `AVAILABILITY ZONES` manually — these will
      be optimally auto-assigned when none are provided.

## Configure the AWS PrivateLink service

1. Retrieve the AWS principal for the AWS PrivateLink connection you just created:

    ```mzsql
    SELECT principal
    FROM mz_aws_privatelink_connections plc
    JOIN mz_connections c ON plc.id = c.id
    WHERE c.name = 'privatelink_svc';
    ```

    ```
                                     principal
    ---------------------------------------------------------------------------
     arn:aws:iam::664411391173:role/mz_20273b7c-2bbe-42b8-8c36-8cc179e9bbc3_u1
    ```

    Follow the instructions in the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/add-endpoint-service-permissions.html)
    to configure your VPC endpoint service to accept connections from the
    provided AWS principal.

1. If your AWS PrivateLink service is configured to require acceptance of connection requests, you must manually approve the connection request from Materialize after executing `CREATE CONNECTION`. For more details, check the [AWS PrivateLink documentation](https://docs.aws.amazon.com/vpc/latest/privatelink/configure-endpoint-service.html#accept-reject-connection-requests).

    **Note:** It might take some time for the endpoint service connection to show up, so you would need to wait for the endpoint service connection to be ready before you create a source.

## Validate the AWS PrivateLink connection

Validate the AWS PrivateLink connection you created using the [`VALIDATE CONNECTION`](/sql/validate-connection) command:

```mzsql
VALIDATE CONNECTION privatelink_svc;
```

If no validation error is returned, move to the next step.

## Create a source connection

In Materialize, create a source connection that uses the AWS PrivateLink connection you just configured:

```mzsql
CREATE CONNECTION mysql_connection TO MYSQL (
      HOST <host>,
      PORT 3306,
      USER 'materialize',
      PASSWORD SECRET mysqlpass,
      SSL MODE REQUIRED,
      AWS PRIVATELINK privatelink_svc
);
```

This MySQL connection can then be reused across multiple [`CREATE SOURCE`](https://materialize.com/docs/sql/create-source/mysql/) statements.
