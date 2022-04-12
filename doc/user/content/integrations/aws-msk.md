---
title: "Configure AWS MSK"
description: "How to configure AWS MSK to Materialize"
weight:
menu:
  main:
    parent: guides
---

This guide goes through the required steps to connect Materialize to an AWS MSK cluster, including some of the more complicated bits around configuring security settings in MSK.

The process contains the following steps:
1. #### Create an Amazon MSK cluster
    a. Sign in to the AWS Management console and open the [Amazon MSK console](https://us-east-1.console.aws.amazon.com/msk/)

    b. Choose **Create cluster**

    c. Enter a cluster name, and leave all other settings unchanged

    d. From the table under **All cluster settings**, copy the values of the following settings and save them because you need them later in this tutorial: **VPC**, **Subnets**, **Security groups associated with VPC**

    e. Choose **Create cluster**

    **Note:** This creation can take about 15 minutes.

2. #### Make the cluster public and enable SASL
    ##### Turn on SASL
    a. Navigate to the [Amazon MSK console](https://us-east-1.console.aws.amazon.com/msk/)

    b. Choose the MSK cluster you just created in Step 1

    c. Click on the **Properties** tab

    d. In the **Security settings** section, choose **Edit**

    e. Check the checkbox next to **SASL/SCRAM authentication**

    f. Click **Save changes**

    You can find more details about updating an MSK cluster's security configurations [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-update-security.html).

    ##### Create a symmetric key
    a. Now go to the [Amazon KMS (Key Management Service) console](https://us-east-1.console.aws.amazon.com/kms)

    b. Click **Create Key**

    c. Choose **Symmetric** and click **Next**

    d. Give the key and **Alias** and click **Next**

    e. Under Administrative permissions, check the checkbox next to the **AWSServiceRoleForKafka** and click **Next**

    f. Under Key usage permissions, again check the checkbox next to the **AWSServiceRoleForKafka** and click **Next**

    g. Click on **Create secret**

    h. Review the details and click **Finish**

    You can find more details about creating a symmetric cmk [here](https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html#create-symmetric-cmk).

    ##### Store a new Secret
    a. Go to the [AWS Secrets Manager console](https://us-east-1.console.aws.amazon.com/secretsmanager/)

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

    f. On the next page, give a **Secret name** that starts with **AmazonMSK_**

    g. Go forward to the next steps and finish creating the secret. Once created, record the ARN (Amazon Resource Name) value for your secret

    You can find more details about creating a secret using AWS Secrets Manager [here](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html).

    ##### Associate secret with MSK cluster
    a. Navigate back to the [Amazon MSK console](https://us-east-1.console.aws.amazon.com/msk/) and click on the cluster you created in Step 1

    b. Click on the **Properties** tab

    c. In the **Security settings** section, under **SASL/SCRAM authentication**, click on **Associate secrets**

    d. Paste the ARN you recorded in the previous subsection and click **Associate secrets**

    ##### Create the cluster's configuration
    a. Go to the [Amazon CloudShell console](https://us-east-1.console.aws.amazon.com/cloudshell/)

    b. Create a file with the following line
      ```
        allow.everyone.if.no.acl.found = false
      ```

    c. Run the following AWS CLI command, replacing `<config-file-path>` with the path to the file where you saved your configuration in the previous step
    ```
      aws kafka create-configuration --name "ExampleConfigurationName" --description "Example configuration description." --kafka-versions "2.6.2" --server-properties fileb://<config-file-path>
    ```


    You can find more information about making your MSK cluster public [here](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html).

3. #### Create a client machine
    a. Open the [Amazon EC2 console](https://console.aws.amazon.com/ec2/)

    b. Choose **Launch instances**

    c. Choose **Select** to create an instance of _Amazon Linux 2 AMI (HVM) - Kernel 5.10, SSD Volume Type_.

    d. Choose the _t2.xlarge_ instance type by selecting the check box next to it

    e. Choose **Next: Configure Instance Details**

    f. In the **Network** list, choose the VPC whose ID you saved while creating the Amazon MSK Cluster in the first step

    g. In the **Auto-assign Public IP** list, choose _Enable_.

    h. In the menu near the top, choose **Add Tags**.

    i. Choose **Add Tag**.

    j. Enter _Name_ for the **Key** and what you want to name the client for the **Value**.

    k. Choose **Review and Launch**, and then choose **Launch**.

    l. Choose **Create a new key pair**, enter a name for **Key pair name**, and then choose **Download Key Pair**. Alternatively, you can use an existing key pair if you prefer.

    m. Read the acknowledgement, select the check box next to it, and choose **Launch Instances**.

    n. Choose **View Instances**. Then, in the **Security Groups** column, choose the security group that is associated with your client instance.

    o. Copy the name of the security group, and save it for later.

    p. Open the [Amazon VPC console](https://console.aws.amazon.com/vpc/).

    q. In the navigation pane, choose **Security Groups**. Find the security group whose ID you saved while creating the Amazon MSK Cluster. Choose this row by selecting the check box in the first column.

    r. In the **Inbound Rules** tab, choose **Edit inbound rules**.

    s. Choose **Add rule**.

    t. In the new rule, choose _All traffic_ in the Type column. In the second field in the Source column, select the security group of the client machine. This is the group whose name you saved earlier in this step.

    u. Choose **Save rules**. Now the cluster's security group can accept traffic that comes from the client machine's security group.

    Find more details about creating an EC2 client machine for MSK [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).

4. #### Install Kafka and create a topic
    a. Open the [Amazon EC2 console](https://console.aws.amazon.com/ec2/)

    b. In the navigation pane, choose **Instances**, and then choose the client instance you created earlier by selecting the check box next to it

    c. Choose **Actions**, and then choose **Connect**. Follow the instructions to connect to the client machine

    d. Install Java on the client machine by running the following command:
      ```
        sudo yum install java-1.8.0
      ```

    e. Run the following command to download Apache Kafka
      ```
        wget https://archive.apache.org/dist/kafka/2.6.2/kafka_2.12-2.6.2.tgz
      ```

    f. Run the following command in the directory where you downloaded the TAR file in the previous step
      ```
        tar -xzf kafka_2.12-2.6.2.tgz
      ```

    g. Go to the _kafka_2.12-2.6.2_ directory

    h. Open the [Amazon MSK console](https://console.aws.amazon.com/msk/)

    i. Wait for the status of cluster to become _Active_. This might take several minutes. After the status becomes Active, choose the cluster name. This action takes you to a page where you can see the cluster summary.

    j. Choose **View client information**.

    k. Copy the private endpoint for plaintext authentication and the Apache ZooKeeper connection string (also for plaintext communication)

    l. Run the following command, replacing `<ZookeeperConnectString>` with the string that you obtained in the previous instruction and `<topic-name>` with the name you want to give your topic
    ```
      bin/kafka-topics.sh --create --zookeeper <ZookeeperConnectString> --replication-factor 3 --partitions 1 --topic <topic-name>
    ```
    If the command succeeds, you see the following message: _Created topic \<topic-name>_.

    Find more information about creating a Topic [here](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html).


5. #### Create a Materialized source
    a. Open the [Amazon MSK console](https://console.aws.amazon.com/msk/) and select your cluster

    b. Click on **View client information**

    c. Copy the url under **Private endpoint** and against **SASL/SCRAM**. This will be your `<broker-url>` going forward

    d. [Install](https://materialize.com/docs/install/) and [start](https://materialize.com/docs/get-started/) a Materialize instance locally

    e. From the _psql_ terminal, run the following command. Replace `<kafka-name>` with whatever you want to name your source. The broker url is what you copied in step c of this subsection. The `<topic-name>` is the name of the topic you created in Step 4. The `<your-username>` and `<your-password>` is from _Store a new secret_ under Step 2.
    ```
      CREATE SOURCE <kafka-name>
      FROM KAFKA BROKER '<broker-url>' TOPIC '<topic-name>' WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = '<your-username>',
      sasl_password = '<your-password>'
      )
      key format text
      value format text;
    ```

    f. If the command executes without an error and outputs _CREATE SOURCE_, it means that you have successfully connected Materialize to your MSK cluster.
