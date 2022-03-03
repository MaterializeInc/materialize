---
title: "Security"
description: "Learn about Materialize Cloud security options."
disable_toc: true
disable_toc: true
menu:
  main:
    parent: "cloud"
---

{{< cloud-notice >}}

Materialize Cloud secures your connections by enforcing that clients connect via
[TLS 1.2+](https://en.wikipedia.org/wiki/Transport_Layer_Security), and supports:

* Multi-factor authentication
* Encryption at rest
* Single sign-on integration (SSO)

In the future, the enterprise product will also provide secure network ingress and egress. If you have other specific security requirements, please [let us know](../support).

## Static IP addresses

All Materialize [Cloud deployments](../cloud-deployments/) come with a static IP address.

This gives you the ability to connect your Materialize Cloud deployments to [sources](../../sql/create-source) and [sinks](../../sql/create-sink/) secured with a firewall.

Allowing the static IP address enables the connection from your Materialize Cloud deployments to your sources and sinks.

The specific commands to allow the static IP address will vary depending on your operating system and firewall (e.g. `iptables`, `firewall-cmd`, UFW, AWS, Azure, and etc). Please refer to the appropriate documentation for your specific firewall.

### Getting the static IP address

Once you’ve created a [deployment](../cloud-deployments/), you can get its associated static IP address following these steps:

1. Click on a deployment to see the individual deployment view.
2. On the right-hand side, click on the copy button to copy the IP address.

### Allowing the static IP address

The following is a list of commands to allow the static IP address with some of the most common firewalls:

#### iptables
```
sudo iptables -I INPUT -s <static IP> -j ACCEPT
```

#### FIREWALL-CMD

```
sudo firewall-cmd --zone=public --add-source=<static IP>
```

#### UFW

```
sudo ufw allow from <static IP>
```

#### CSF

```
sudo csf -a <static IP>
```

#### AWS Security Groups

If you are already using AWS security groups, you can add the static IP address to the security group to allow connections from your Materialize Cloud deployments.

To add the static IP address to your AWS Security Group, follow these steps:

1. Log into your AWS account.
1. Go to your **Security Groups** and click on the **Security Group** you want to add the static IP address to.
1. Click on the **Edit Inbound Rules** button.
1. Click on the **Add rule** button.
1. Select **Custom TCP** as the type.
1. Specify the **Port range** depending on the source you want to connect to. For example, if you want to connect to a Kafka source, you would specify **Port range: 9092** or if you want to connect to a PostgreSQL source, you would specify **Port range: 5432**.
1. Add the Materialize Cloud deployment’s static IP address followed by `/32` to the **Source** field. The `/32` mask is used to only designate the speficic host, and not the entire subnet.
1. Click on the **Save** button.

For more information on AWS security groups, please refer to the AWS [VPC security groups documentaiton](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html).


#### Azure Firewall

For the steps required to allow static IP addresses with Azure, please refer to the [Azure documentation](https://docs.microsoft.com/en-us/azure/virtual-network/ip-services/configure-public-ip-firewall).


#### GCP VPC firewall

For the steps required to allow static IP addresses with GCP, please refer to the [GCP documentation](https://cloud.google.com/vpc/docs/using-firewalls).
