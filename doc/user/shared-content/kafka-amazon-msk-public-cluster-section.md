This section goes through the required steps to connect Materialize to an Amazon MSK cluster, including some of the more complicated bits around configuring security settings in Amazon MSK.

If you already have an Amazon MSK cluster, you can skip step 1 and directly move
on to **Make the cluster public and enable SASL** step. You can also skip steps
3 and 4 if you already have Apache Kafka installed and running, and have created
a topic that you want to create a source for.

The process to connect Materialize to Amazon MSK consists of the following steps:
1. Create an Amazon MSK cluster. If you already have an Amazon MSK cluster set
   up, then you can skip this step.

    a. Sign in to the AWS Management Console and open the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    b. Choose **Create cluster**

    c. Enter a cluster name, and leave all other settings unchanged

    d. From the table under **All cluster settings**, copy the values of the following settings and save them because you need them later in this tutorial: **VPC**, **Subnets**, **Security groups associated with VPC**

    e. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

1. Make the cluster public and enable SASL.
    ##### Turn on SASL
    a. Navigate to the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    b. Choose the MSK cluster you just created in Step 1

    c. Click on the **Properties** tab

    d. In the **Security settings** section, choose **Edit**

    e. Check the checkbox next to **SASL/SCRAM authentication**

    f. Click **Save changes**

    You can find more details about updating a cluster's security configurations [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html).

    ##### Create a symmetric key
    a. Now go to the [AWS Key Management Service (AWS KMS) console](https://console.aws.amazon.com/kms)

    b. Click **Create Key**

    c. Choose **Symmetric** and click **Next**

    d. Give the key and **Alias** and click **Next**

    e. Under Administrative permissions, check the checkbox next to the **AWSServiceRoleForKafka** and click **Next**

    f. Under Key usage permissions, again check the checkbox next to the **AWSServiceRoleForKafka** and click **Next**

    g. Click on **Create secret**

    h. Review the details and click **Finish**

    You can find more details about creating a symmetric key [here](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk).

    ##### Store a new Secret
    a. Go to the [AWS Secrets Manager console](https://console.aws.amazon.com/secretsmanager/)

    b. Click **Store a new secret**

    c. Choose **Other type of secret** (e.g. API key) for the secret type

    d. Under **Key/value pairs** click on **Plaintext**

    e. Paste the following in the space below it and replace `<your-username>` and `<your-password>` with the username and password you want to set for the cluster
      ```
        {
          "username": "<your-username>",
          "password": "<your-password>"
        }
      ```

    f. On the next page, give a **Secret name** that starts with `AmazonMSK_`

    g. Under **Encryption Key**, select the symmetric key you just created in the previous sub-section from the dropdown

    h. Go forward to the next steps and finish creating the secret. Once created, record the ARN (Amazon Resource Name) value for your secret

    You can find more details about creating a secret using AWS Secrets Manager [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html).

    ##### Associate secret with MSK cluster
    a. Navigate back to the [Amazon MSK console](https://console.aws.amazon.com/msk/) and click on the cluster you created in Step 1

    b. Click on the **Properties** tab

    c. In the **Security settings** section, under **SASL/SCRAM authentication**, click on **Associate secrets**

    d. Paste the ARN you recorded in the previous subsection and click **Associate secrets**

    ##### Create the cluster's configuration
    a. Go to the [Amazon CloudShell console](https://console.aws.amazon.com/cloudshell/)

    b. Create a file (eg. _msk-config.txt_) with the following line
      ```
        allow.everyone.if.no.acl.found = false
      ```

    c. Run the following AWS CLI command, replacing `<config-file-path>` with the path to the file where you saved your configuration in the previous step
    ```
      aws kafka create-configuration --name "MakePublic" \
      --description "Set allow.everyone.if.no.acl.found = false" \
      --kafka-versions "2.6.2" \
      --server-properties fileb://<config-file-path>/msk-config.txt
    ```

    You can find more information about making your cluster public [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

1. If you already have a client machine set up that can interact with your
   cluster, then you can skip this step.

    If not, you can create an EC2 client machine and then add the security group of the client to the inbound rules of the cluster's security group from the VPC console. You can find more details about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

1. Install Apache Kafka and create a topic. To start using Materialize with
    Apache Kafka, you need to create a Materialize source over an Apache Kafka
    topic. If you already have Apache Kafka installed and a topic created, you
    can skip this step.

    Otherwise, you can install Apache Kafka on your client machine from the previous step and create a topic. You can find more information about how to do that [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).

1. Create ACLs. As `allow.everyone.if.no.acl.found` is set to `false`, you must
    create ACLs for the cluster and topics configured in the previous step to
    set appropriate access permissions. For more information, see the [Amazon
    MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-acls.html)
    documentation.


1. Create a connection in Materialize.

    a. Open the [Amazon MSK console](https://console.aws.amazon.com/msk/) and select your cluster

    b. Click on **View client information**

    c. Copy the url under **Private endpoint** and against **SASL/SCRAM**. This will be your `<broker-url>` going forward.

    d. Connect to Materialize using the [SQL Shell](/console/),
       or your preferred SQL client.

    e. Create a connection using the command below. The broker URL is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` is from _Store a new secret_ under Step 2.

      ```mzsql
      CREATE SECRET msk_password AS '<your-password>';

      CREATE CONNECTION kafka_connection TO KAFKA (
          BROKER '<broker-url>',
          SASL MECHANISMS = 'SCRAM-SHA-512',
          SASL USERNAME = '<your-username>',
          SASL PASSWORD = SECRET msk_password
        );
      ```
