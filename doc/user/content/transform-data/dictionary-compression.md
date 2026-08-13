---
title: "Dictionary compression"
description: "Reduce arrangement memory for columns that hold a small set of repeated values."
menu:
  main:
    name: "Dictionary compression"
    parent: transform-data
    identifier: dictionary-compression
    weight: 35
---

{{< public-preview >}}
Arrangement dictionary compression
{{< /public-preview >}}

{{% include-headless "/headless/dictionary-compression/overview" %}}

Within a column, the values that repeat most often are the ones Materialize
stores once and references. Everything else is stored as-is, exactly as it would
be without compression. Materialize applies compression per column and decides
which columns to compress, so a wide row can have one column compressed and the
rest untouched.

## Enable dictionary compression

Set the `EXPERIMENTAL ARRANGEMENT COMPRESSION` option on each managed cluster
whose arrangements you want compressed.

The option is configured on the cluster, but the arrangements it applies to live
in the cluster's replicas, in each replica's memory. A replica picks up the
configured value when it is created and holds it for its lifetime.

{{% include-headless "/headless/dictionary-compression/replica-replacement" %}}

### Set the option on a cluster

You can set the option when you create a cluster:

```mzsql
CREATE CLUSTER my_cluster (
    SIZE = '100cc',
    EXPERIMENTAL ARRANGEMENT COMPRESSION = true
);
```

Or change it on an existing cluster:

```mzsql
ALTER CLUSTER my_cluster SET (EXPERIMENTAL ARRANGEMENT COMPRESSION = true);
```

To go back to the default:

```mzsql
ALTER CLUSTER my_cluster RESET (EXPERIMENTAL ARRANGEMENT COMPRESSION);
```

[`SHOW CREATE CLUSTER`] reports the configured value.

## The tradeoff

Dictionary compression trades CPU for memory, and it does **not** reduce memory
on every workload. Use this section to judge whether it suits your workload, and
[contact our team](/support/) if you have questions.

{{< note >}}
A single arrangement usually holds both columns that compress well and columns
that do not. Only the columns that compress save memory, and every column still
pays the CPU cost. Materialize decides which columns get compressed, and you
cannot select them yourself. You turn compression on per cluster, and the
optimizer chooses which intermediate arrangements a dataflow builds.
{{< /note >}}

### When it helps

- Columns that hold a small set of often-repeated, longer values. Typical
  examples are status strings, enum-like labels, country codes, and tenant IDs.
  Most or all of the memory savings come from columns like these. See [How
  distinct values affect the benefit](#how-distinct-values-affect-the-benefit)
  for how the benefit changes as that set grows.
- Large arrangements. Compression saves more memory when repeated values occur
  across more rows, so larger arrangements generally offer more opportunity for
  savings.

Only data held in an arrangement is affected. That means [indexes] and the
arrangements that a dataflow builds internally for joins and aggregations
(`GROUP BY`, `DISTINCT`). Data that is not arranged is untouched. A materialized
view's stored result is not an arrangement, but the dataflow that maintains it
builds these internal arrangements for any joins and aggregations it computes.

### When it does not help

- **High-cardinality or near-unique columns.** Unique values are not worth
  storing in a dictionary, so columns dominated by them see little or no memory
  savings. Materialize does not detect a high-cardinality column and skip it up
  front. It inspects every column of every row regardless, so a near-unique
  column pays the full CPU cost for little or no memory benefit. Unique
  identifiers, timestamps, and free-form text are typical examples.
- **Columns of short values.** Booleans, `NULL`s, and small integers are already
  stored compactly enough that a dictionary reference cannot beat storing the
  value itself. They are never compressed.
- **Small arrangements.** An arrangement built from scratch does not install a
  dictionary until it has seen on the order of 65,000 rows. Small arrangements
  are effectively unaffected.

### The CPU cost

The cost falls mainly on the write path. As updates arrive, Materialize keeps
approximate counts of which values repeat most in each column and maintains the
dictionary. The most
visible symptom is slower arrangement hydration. A replica in a cluster with
compression enabled takes longer to build its arrangements after it is created,
restarted, or resized.

Reads pay a smaller but ongoing cost. Resolving a compressed value requires an
extra indirection, and comparing compressed rows cannot use the fast path that
uncompressed rows use.

## How distinct values affect the benefit

The memory benefit is largest when a column repeats a small set of values across
many rows. It tapers off as the number of distinct values in a column grows past
roughly 64, and with enough distinct values the bookkeeping becomes overhead that
buys no memory saving at all.

This is not a limit. Nothing breaks when a column holds more distinct values
than that. Values that are not worth storing in a dictionary, including values
that appear only once, are stored as-is. The result is less memory saved, and
nothing else changes.

## Observe the effect

There is no metric or system catalog relation specific to dictionary
compression. The effect shows up in the existing arrangement-size introspection:

```mzsql
SELECT name, records, size
FROM mz_introspection.mz_dataflow_arrangement_sizes
ORDER BY size DESC;
```

See [`mz_introspection.mz_arrangement_sizes`] for per-operator numbers and
[`mz_introspection.mz_dataflow_arrangement_sizes`] for per-dataflow numbers. The
reported size includes the dictionary's own overhead, so on a workload that does
not compress well you may see the cost of the machinery rather than a saving.

To judge whether compression helped, compare arrangement sizes for the same
objects with the option on and off. For example, run the same workload on a
second cluster with the option set differently. Because changing the option
re-hydrates the cluster, wait until hydration has completed before you measure.

## Related pages

- [Arrangements]
- [Query optimization](/transform-data/optimization/)
- [Dataflow troubleshooting](/transform-data/dataflow-troubleshooting/)
- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [`SHOW CREATE CLUSTER`]

[Arrangements]: /get-started/arrangements/
[indexes]: /concepts/indexes/
[`SHOW CREATE CLUSTER`]: /sql/show-create-cluster/
[`mz_introspection.mz_arrangement_sizes`]: /reference/system-catalog/mz_introspection/#mz_arrangement_sizes
[`mz_introspection.mz_dataflow_arrangement_sizes`]: /reference/system-catalog/mz_introspection/#mz_dataflow_arrangement_sizes
