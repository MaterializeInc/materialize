---
title: "Account Limits"
description: "Learn what features Materialize Cloud offers."
menu:
  main:
    parent: "cloud"
    weight:
---

{{< cloud-notice >}}

Materialize Cloud offers two deployment sizes for free trials, with additional processing and memory capacity available for enterprise accounts. We use AWS as the cloud service provider and the us-east-1 datacenter.

Trials last for 30 days or until you max out the available resources for free deployments, whichever comes first. Accounts and deployments may be deleted 14 days after your trial expires unless you have upgraded to an enterprise account.

## Materialize Cloud specifications

Deployment Size | XS | S | M | L | XL
----------------|----|---|---|---|---
**Level**  | free  | free  | enterprise  | enterprise  |  enterprise
**CPUs**  | 4vCPUs  | 8vCPUs  | 16vCPUs  |  32vCPUs  |  64vCPUs
**RAM**  |  32GB | 64GB  | 128GB  | 256GB  |  512GB
**R5B instances**  | r5.xlarge   | r5.2xlarge  | r5.4xlarge   | r5.8xlarge   |  r5.16xlarge

If you already know you need a larger deployment size for your use case, [contact us](../support).

## Materialize Cloud vs. Materialized installed

For the most part, Materialize Cloud offers the same functionality as the installed version. The major exceptions are:

* Materialize Cloud doesn't currently support using files as sources or sinks; you can only use streaming sources or sinks.
* Materialize installation deployment limits depend solely on your hardware.
* We reserve the right to terminate a session on your deployment. This may happen after prolonged inactivity or if we need to upgrade Materialize Cloud. Catalog items persist across sessions.

Sources | Materialize Cloud | Materialize local install
--------|-------------------|--------------------------
**Kafka** | Yes | Yes
**Kinesis**  | Yes | Yes
**Postgres**  | Yes | Yes
**PubNub**  | Yes | Yes
**S3**  |  Yes | Yes
**Files**  |  Yes |  No

## Related topics

* [What Is Materialize?](/overview/what-is-materialize)
* [Connect to Materialize Cloud](../connect-to-materialize-cloud)
* [Materialize Cloud Support](../support)
