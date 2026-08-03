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

{{< private-preview >}}
Arrangement dictionary compression
{{< /private-preview >}}

Dictionary compression reduces the memory that [arrangements] use when a column
holds the same values over and over. Instead of storing a repeated value in full
once per row, Materialize stores that value once and has each row reference it.
The values that repeat most often within a column are stored this way, and
everything else is stored as-is, exactly as it would be without compression.
Compression is applied per column, so a wide row can have one column compressed
and the rest untouched.

Dictionary compression is **alpha** and is **off by default at every layer**. It
requires both an environment-wide feature flag that only Materialize can set and
a per-cluster (or per-replica) option, so you cannot enable it on your own.

## The tradeoff

Dictionary compression trades CPU for memory, and it does **not** reduce memory
on every workload. Read this section before asking to have it turned on.

### What it helps

- Columns that hold a **small set of longer values that repeat often**. Status
  strings, enum-like labels, country codes, and tenant IDs are typical examples.
  Essentially all of the savings come from columns like these. See [How many
  distinct values a column should
  have](#how-many-distinct-values-a-column-should-have) for a rule of thumb.
- **Large** arrangements. The larger the arrangement, the more occurrences of
  each repeated value there are to collapse.

Only data held in an arrangement is affected. That means [indexes], materialized
views, and the arrangements that joins and aggregations (`GROUP BY`, `DISTINCT`)
build. Data that is not arranged is untouched.

### What it does not help

- **High-cardinality or near-unique columns.** This is the most important caveat
  on this page. A value that never repeats is never worth storing in a
  dictionary, so such a column sees no memory savings. Materialize has no
  heuristic that detects this and skips the column. It inspects every column of
  every row regardless, so a near-unique column pays the full CPU cost for zero
  memory benefit. If your large arrangements are dominated by unique
  identifiers, timestamps, or free-form text, expect cost without benefit.
- **Columns of short values.** Booleans, `NULL`s, and small integers are already
  stored compactly enough that a dictionary reference cannot beat storing the
  value itself. They are never compressed.
- **Small arrangements.** An arrangement built from scratch does not install a
  dictionary until it has seen on the order of 65,000 rows. Small arrangements
  are effectively unaffected, for better or worse.

### The CPU cost

The cost falls mainly on the **write path**. As updates arrive, Materialize has
to track which values in each column repeat and maintain the dictionary. The
most visible symptom is **slower arrangement hydration**. A cluster with
compression enabled takes longer to bring its indexes and materialized views up
to date after it is created, restarted, or resized.

Reads pay a smaller but ongoing cost. Resolving a compressed value requires an
extra indirection, and comparing compressed rows cannot use the fast path that
uncompressed rows use.

## How many distinct values a column should have

As a rule of thumb, dictionary compression helps most when a column holds fewer
than about 64 distinct values. Beyond roughly that many, the benefit tapers off,
and as the number of distinct values keeps growing the feature trends toward pure
CPU overhead with no memory saving.

Treat this as guidance about where the feature pays off, not as a limit,
threshold, or capacity. Nothing breaks when a column holds more distinct values.
Values that are not worth storing in a dictionary, including values that appear
only once, are simply stored as-is. You see less memory savings and nothing else
changes.

## Enable dictionary compression

Two settings are required, and compression happens only when **both** are on.

1. **The environment-wide feature flag**
   `enable_arrangement_dictionary_compression_alpha`. It defaults to off and is
   not user-settable. It can only be changed with `ALTER SYSTEM SET` as the
   `mz_system` user, and it applies to your whole Materialize environment rather
   than to a single cluster. Ask Materialize support or your field engineer to
   enable it.

2. **The per-cluster or per-replica option**
   `EXPERIMENTAL ARRANGEMENT COMPRESSION`, which also defaults to off. This is
   the setting that decides *which* clusters use compression once the flag is
   on.

Because both layers must be on, having the flag enabled for your environment
does not change any cluster until you also request compression on that cluster.

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

The same option is available on [`CREATE CLUSTER REPLICA`]. It is not supported
on unmanaged clusters. [`SHOW CREATE CLUSTER`] reports the configured value.

{{< warning >}}
Changing `EXPERIMENTAL ARRANGEMENT COMPRESSION` on a cluster **replaces the
cluster's replicas**, so the cluster re-hydrates. Plan for this the same way you
would plan for resizing a cluster. The cluster has to rebuild its indexes and
materialized views before it is fully caught up again, and hydration is slower
with compression enabled.
{{< /warning >}}

The option is accepted and stored whether or not the environment-wide flag is
enabled. The flag decides whether a replica actually honors the value. A replica
captures the setting once, when it is created, and holds it for its lifetime, so
turning the flag on or off changes behavior only for replicas created
afterwards. For the same reason, a single cluster can hold a mix of compressed
and uncompressed arrangements.

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
- [`CREATE CLUSTER REPLICA`]
- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [`SHOW CREATE CLUSTER`]

[arrangements]: /get-started/arrangements/#arrangements
[Arrangements]: /get-started/arrangements/
[indexes]: /concepts/indexes/
[`CREATE CLUSTER REPLICA`]: /sql/create-cluster-replica/
[`SHOW CREATE CLUSTER`]: /sql/show-create-cluster/
[`mz_introspection.mz_arrangement_sizes`]: /reference/system-catalog/mz_introspection/#mz_arrangement_sizes
[`mz_introspection.mz_dataflow_arrangement_sizes`]: /reference/system-catalog/mz_introspection/#mz_dataflow_arrangement_sizes
