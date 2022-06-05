---
title: "Account limits"
description: "Learn what features Materialize Cloud offers."
menu:
  main:
    parent: "cloud"
---

{{< cloud-notice >}}

The Materialize Cloud trial period lasts for **30 days** or **until you max out the available resources** (whichever comes first), and is limited to **two** deployments. Once the trial expires, your account and deployments may be deleted within 14 days, unless you have upgraded to an enterprise account.

## Materialize Cloud specifications

For now, Materialize Cloud is only available on **AWS** within the `us-east-1` and `eu-west-1` [regions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions). We are working on expanding this availability to additional cloud providers and regions soon.

Materialize Cloud offers five deployment sizes (**XS**, **S**, **M**, **L**, **XL**), capacity roughly doubling with each size. The **XS** and **S** sizes are available **for free** during a limited trial period, with larger capacity available for enterprise accounts.

You can find pricing information [here](https://materialize.com/pricing). If you need a larger deployment size for your specific use case, [get in touch with us](../support).

### Cloud vs. self-managed

For the most part, Materialize Cloud offers the same functionality as the self-managed version. The major exceptions to be aware of are:

- Materialize Cloud doesn't support using local files as sources; you can otherwise use any other combination of source type and format [offered by Materialize](/sql/create-source/).
- We reserve the right to terminate a session in your Cloud deployment. This may happen after prolonged inactivity or in the event of planned maintenance work on Materialize Cloud, and doesn't affect catalog items (which are persisted across sessions).

## Related topics

* [What Is Materialize?](/overview/what-is-materialize)
* [Connect to Materialize Cloud](../connect-to-cloud)
* [Materialize Cloud Support](../support)
* [Materialize Cloud Pricing](https://materialize.com/pricing)
