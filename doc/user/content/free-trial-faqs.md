---
title: "Free trial FAQs"
description: "Answers to frequently asked questions about Materialize Cloud free trials"
menu:
  main:
    parent: "about"
    weight: 20
---

When you [sign up for Materialize Cloud](https://materialize.com/register/), you
get a free trial account so you can explore the product and start building! This
page answers some frequently asked questions about free trials.

{{< tip >}}
For help getting started with your data or other questions about Materialize, you can schedule a [free guided trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).
{{< /tip >}}

## What are the limits of a free trial?

In Materialize, [clusters](/concepts/clusters/) are the pools of
compute resources for running your workloads. The size and replication factor
of each cluster determines its [credit usage](/sql/create-cluster/#credit-usage).

During your free trial, the credit consumption rate across all clusters in a
region cannot exceed 4 credits per hour at any point in time. This limit should
accommodate most trial scenarios.

For example, let's say you have 3 clusters in a region:

Cluster     | Size      | Replication factor | Credits per hour
------------|-----------|--------------------|-----------------
`ingest`    | `50cc`    | 1                  | 0.5
`compute`   | `50cc`    | 2                  | 1 (0.5 each)
`quickstart`| `25cc`    | 1                  | 0.25

In this case, your credit consumption rate would be 1.75 credits per hour, which
is under the rate limit of 4 credits per hour.

## How long does a free trial last?

7 days. If you need additional time, please [chat with our team](https://materialize.com/convert-account/?utm_campaign=General&utm_source=documentation).

To continue using Materialize, you can upgrade to a paid, [On Demand
plan](https://materialize.com/pdfs/on-demand-terms.pdf) from the billing section
of the [Materialize console](/console/). Otherwise,
Materialize will delete your resources and data at the end of the trial period.

## How do I monitor my credit consumption rate?

To see your current credit consumption rate, measured in credits per hour, run
the following query against Materialize:

```mzsql
SELECT sum(s.credits_per_hour) AS credit_consumption_rate
  FROM mz_cluster_replicas r
  JOIN mz_cluster_replica_sizes s ON r.size = s.size;
```

For example, if you start your free trial by following the [getting started guide](/get-started/quickstart),
or if you otherwise only use the pre-installed `quickstart` cluster
(`25cc`), you will end up consuming `.25` credit per hour:

```nofmt
 credit_consumption_rate
-------------------------
                     .25
(1 row)
```

## Can I go over the credit rate limit?

No, you cannot go over the rate limit of 4 credits per hour at any time during
your free trial. If you try to add a replica that puts you over the limit,
Materialize will return an error similar to:

```nofmt
Error: creating cluster replica would violate max_credit_consumption_rate limit (desired: 6, limit: 4, current: 3)
Hint: Drop an existing cluster replica or contact support to request a limit increase.
```

If you need additional resources during your trial, [chat with our team](http://materialize.com/convert-account/?utm_campaign=General&utm_source=documentation).

## How do I get help during my trial?

If you have questions about Materialize or need support, reach out to us in our
[Community Slack](https://materialize.com/s/chat).
