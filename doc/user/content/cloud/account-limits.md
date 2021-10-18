---
title: "Account Limits"
description: "Learn what features Materialize Cloud offers."
menu:
  main:
    parent: "cloud"
    weight: 3
---

{{< cloud-notice >}}

Materialize Cloud offers five deployment sizes with increasing processing and memory capacities. The **XS** and **S** sizes are available **for free** during a limited trial period, with larger capacity available for enterprise accounts.

The trial period lasts for **30 days** or **until you max out the available resources** (whichever comes first), and is limited to **two** deployments. Once the trial expires, your account and deployments may be deleted within 14 days, unless you have upgraded to an enterprise account.

## Materialize Cloud specifications

For now, Materialize Cloud is only available on **AWS** within the `us-east-1` [region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions). We are working on expanding this availability to additional cloud providers and regions soon.

Deployment Size | CPUs | RAM
----------------|------|----
**XS** | 4vCPUs | 32GB
**S** | 8vCPUs   | 64GB
**M**  | 16vCPUs   |  128GB
**L**  | 32vCPUs   |  256GB
**XL**  | 64vCPUs   |  512GB

You can find pricing information [here](https://materialize.com/pricing). If you need a larger deployment size for your specific use case, [get in touch with us](../support).

### Cloud vs. self-managed

For the most part, Materialize Cloud offers the same functionality as the self-managed version. The major exceptions to be aware of are:

#### Sources

Materialize Cloud doesn't support using local files as sources; you can otherwise use any other combination of [source type](/sql/create-source/#types-of-sources) and format:

  Sources | Materialize Cloud | Materialize
  --------|-------------------|--------------------------
  **Kafka** | Yes | Yes
  **Kinesis**  | Yes | Yes
  **S3**  |  Yes | Yes
  **PubNub**  | Yes | Yes
  **Local Files**  |  No |  Yes
  **Postgres**  | Yes | Yes

#### Session termination

We reserve the right to terminate a session in your Cloud deployment. This may happen after prolonged inactivity or in the event of planned maintenance work on Materialize Cloud, and doesn't affect catalog items (which are persisted across sessions).

## Related topics

* [What Is Materialize?](/overview/what-is-materialize)
* [Connect to Materialize Cloud](../connect-to-cloud)
* [Materialize Cloud Support](../support)
* [Materialize Cloud Pricing](https://materialize.com/pricing)
