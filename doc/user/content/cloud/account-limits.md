---
title: "Materialize Cloud Account Limits"
description: "Learn Materialize Cloud's account limits and its differences from the installed version."
menu:
  main:
    parent: "cloud"
    weight: 3
---

{{< cloud-notice >}}

Materialize Cloud offers two deployment sizes for free trials, with additional processing and RAM available for a premium upgrade. We use AWS as the cloud service provider and the us-east-1 datacenter.

Trials last for 30 days or until you max out the available resources for free deployments, whichever comes first. Accounts and deployments will be deleted 14 days after your trial expires unless you have upgraded to a premium version.

## Materialize Cloud specifications

Deployment Size | XS | S | M | L | XL
----------------|----|---|---|---|---
**Level**  | free  | free  | premium  | premium  |  premium
**No. deployments** | 2  | 2  | ?? | ??  |  ??
**CPUs**  | 4vCPUs  | 8vCPUs  | 16vCPUs  |  32vCPUs  |  64vCPUs
**RAM**  |  32GB | 64GB  | 128GB  | 256GB  |  512GB
**R5B instances**  | r5.xlarge   | r5.2xlarge  | r5.4xlarge   | r5.8xlarge   |  r5.16xlarge
**Auth**  | Github, Google  | Github, Google  | Github, Google, 2FA, SSO  | Github, Google, 2FA, SSO   |  Github, Google, 2FA, SSO

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
