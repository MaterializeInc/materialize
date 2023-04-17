---
title: "Free trial FAQs"
description: "Answers to frequently asked questions about Materialize free trials"
menu:
  main:
    parent: "quickstarts"
    weight: 15
---

When you [sign up for Materialize](https://materialize.com/register/), you get a free trial account so you can explore the product and start building! This page answers some frequently asked questions about free trials.

## What are the limits of a free trial?

In Materialize, [cluster replicas](/overview/key-concepts/#cluster-replicas) are the physical resources for doing computational work. Cluster replicas come in various sizes, and each size has a [credit consumption rate](https://materialize.com/pricing/).

During your free trial, your credit consumption rate across all replicas in a region cannot exceed 4 credits per hour at any point in time. This limit should accommodate most trial scenarios.

For example, let's say you have 3 clusters in a region:

Cluster | Replicas | Credits per hour
--------|----------|-----------------
`ingest`| 1 `2xsmall` | 0.5
`compute` | 2 `2xsmall` | 1 (0.5 each)
`default` | 1 `2xsmall` | 0.5

In this case, your credit consumption rate would be 2 credits per hour, which is under the rate limit of 4 credits per hour.

## How long does a free trial last?

14 days.

If you need additional time, please [chat with our team](http://materialize.com/convert-account/). Otherwise, Materialize will delete your resources and data at the end of the trial period.

## How do I monitor my credit consumption rate?

To see your current credit consumption rate, measured in credits per hour, run the following query:

```sql
SELECT sum(s.credits_per_hour) AS credit_consumption_rate
  FROM mz_cluster_replicas r
  JOIN mz_internal.mz_cluster_replica_sizes s ON r.size = s.size;
```

For example, if you start your free trial with the [getting started guide](/get-started/), you'll end up consuming credits at a rate of `3.5` per hour:

```nofmt
 credit_consumption_rate
-------------------------
                     3.5
(1 row)
```

## Can I go over the credit rate limit?

No, you cannot go over the rate limit of 4 credits per hour at any time during your free trial. If you try to add a replica that puts you over the limit, Materialize will return an error.

If you need additional resources during your trial, [chat with our team](http://materialize.com/convert-account/).

## How do I get help during my trial?

If you have questions about Materialize or need support, reach out to us in our [Community Slack](https://materialize.com/s/chat).
