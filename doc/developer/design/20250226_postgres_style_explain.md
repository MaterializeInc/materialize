# Postgres-style syntax for `EXPLAIN`

- Associated: https://github.com/MaterializeInc/database-issues/issues/8889

## The Problem

`EXPLAIN` is meant to help users understand how Materialize actually
runs their queries. In the name of streamlining the education process,
we should make our output as much like Postgres's as is practicable.

Changing `EXPLAIN` is tricky, though: we rely heavily on `EXPLAIN`'s
completionist output to test our optimizer and debug queries. We must
be careful to keep these tests while enabling the new behavior.

[#31185](https://github.com/MaterializeInc/materialize/pull/31185) laid
the groundwork for updating what `AS TEXT` means. So: what should it mean?

## Success Criteria

Our default `EXPLAIN` output should be concise and in a format
reminiscent of Postgres's. Ideally, `EXPLAIN` output should match the
output in `mz_lir_mapping`. The documentation should reflect this new
syntax.

## Out of Scope

We are not going to build new `EXPLAIN` infrastructure, diagrams,
etc. For example, we are not going to attempt to differentiate between
the different meanings of `ArrangeBy` in MIR.

We are not going to invent fundamentally new ways of explaining
how Materialize works.

We are not going to do a user study in advance of any changes. (But we
will listen attentively to feedback!)

## Solution Proposal

Postgres explain plans have the format:

```
Operator
  Detail
  -> Child Operator #1
     Detail
     ...
  -> Child Operator #2
     Detail
     ...
```

We should aim to follow Postgres's norms: operator names spelled out
with spaces, and properties are clearly elucidated in human-readable
formats. When it is sensible, we have simply borrowed Postgres's
terminology, i.e., `Reduce` is renamed to `GroupAggregate`.

The guiding principle here is that every operator is of the form
`(Adjective) Operator`, with lines below offering more detail.  We
should choose `Operator` to use familiar and evocative terminology
(knowing that we can't always follow Postgres, because our execution
models are so different). We should choose `Adjective` such that
expensive moments---allocation, arrangement---are called
out/searchable.

Postgres displays some parts of the query differently from us, namely:

  - Column names:
    + When a column name is available, it just gives the name (no number).
    + When a column name is unavailable, it gives the number using `$2`.
  - `Map` and `Project` do not appear

We will use LIR as the new default `EXPLAIN`/`EXPLAIN AS TEXT`
output. We will update `mz_lir_mapping` to use the new Postgres-style
syntax (fixing [a bug with `Let` and `LetRec`
rendering](https://github.com/MaterializeInc/database-issues/issues/8993)
in the process.

We will need three pieces of work, which should all land together:

 - We must implement the new output and put in appropriate SLT tests
   for it.
 - We must update `mz_lir_mapping` to use the new vocabulary.
 - We must update the documentation to explain the new output, ideally
   using this output everywhere `EXPLAIN` is used.

### Concrete Mapping

| LIR node    | `mz_lir_mapping` node                    | New, Postgres-style syntax                           |
| :---------- | :--------------------------------------- | :--------------------------------------------------- |
| `Constant`  | `Constant`                               | `Constant`                                           |
| `Get`       | `Get::PassArrangements l0`               | `Index Scan on l0 using ...` or `Stream Scan on l0`  |
| `Get`       | `Get::Arrangement l0 (val=...)`          | `Index Lookup on l0 using ...`                       |
| `Get`       | `Get::Arrangement l0`                    | `Index Scan on l0 using ...` (showing mfp)           |
| `Get`       | `Get::Collection l0`                     | `Read l0`                                            |
| `Mfp`       | `MapFilterProject`                       | `Map/Filter/Project`                                 |
| `FlatMap`   | `FlatMap`                                | `Table Function`                                     |
| `Join`      | `Join::Differential`                     | `Differential Join`                                  |
| `Join`      | `Join::Delta`                            | `Delta Join`                                         |
| `Reduce`    | `Reduce::Distinct`                       | `Distinct GroupAggregate`                            |
| `Reduce`    | `Reduce::Accumulable`                    | `Accumulable GroupAggregate`                         |
| `Reduce`    | `Reduce::Hierarchical (monotonic)`       | `Monotonic Hierarchical GroupAggregate`              |
| `Reduce`    | `Reduce::Hierarchical (buckets: ...)`    | `Bucketed Hierarchical GroupAggregate`               |
| `Reduce`    | `Reduce::Basic`                          | `Non-incremental GroupAggregate`                     |
| `Reduce`    | `Reduce::Collation`                      | `Collated GroupAggregate` (details?)                 |
| `TopK`      | `TopK::MonotonicTop1`                    | `Monotonic Top1`                                     |
| `TopK`      | `TopK::MonotonicTopK`                    | `Monotonic TopK`                                     |
| `TopK`      | `TopK::Basic`                            | `Non-monotonic TopK`                                 |
| `Negate`    | `Negate`                                 | `Negate Diffs`                                       |
| `Threshold` | `Threshold`                              | `Threshold Diffs`                                    |
| `Union`     | `Union`                                  | `Union`                                              |
| `Union`     | `Union (consolidates output)`            | `Consolidating Union`                                |
| `ArrangeBy` | `Arrange`                                | `Arrange` or `Stream/Arrange`                        |
| `Let`       | `e0 With l1 = e1 ...`                    | `e1 With l1 = e1 ...`                                |
| `LetRec`    | `e0 With Mutually Recursive l1 = e1 ...` | `e0 With Mutually Recursve l1 = e1 ...`              |

Notice that we have used the following "expensive" adjectives:
`Non-incremental`, `Bucketed`, `Non-monotonic`, `Consolidating`.

In the new Postgres-style syntax, extra information will appear on the
next line: for joins, it will be the join pipelines; for
`Map/Filter/Project` it will be the expressions used in the maps and
filters.

For `Delta Join` in particular, we will want to push information
further down in the listing; see [TPC-H query 3](#tpc-h-query-3) below
for an example.

### Formerly Open Questions

**Should we show `Project`? Should we show _all_ expressions for `Map`
and `Filter`?** Yes: we will show all Mfp expressions by default.

**How much of this data should `mz_lir_mapping` show?** I propose
showing the first line plus anything involving scalar expressions
(e.g., `Map/Filter/Project`s, `Join` equivalences, etc.).

**What about names?** Separate efforts
([#31802](https://github.com/MaterializeInc/materialize/pull/31802)
will help us [get more column
names](https://github.com/MaterializeInc/database-issues/issues/8960)). Showing
_only_ column names (without numbers) can induce some confusion when
we have self-joins, as in outer-join lowering. We will want to add
context (e.g., the table alias, `f1.f_col = f2.f_col`).

## Minimal Viable Prototype

These examples are adapted from existing MIR explain plans, so they
are not completely faithful to the language above (e.g., `Map` and
`Filter` are separate, when they will be combined in
`Map/Filter/Project`).

Arity is included in the Postgres style (cf. "width="), though we will
hopefully not need it when we have good column names.

### TPC-H query 1

The query:

```sql
SELECT
	l_returnflag,
	l_linestatus,
	sum(l_quantity) AS sum_qty,
	sum(l_extendedprice) AS sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
	avg(l_quantity) AS avg_qty,
	avg(l_extendedprice) AS avg_price,
	avg(l_discount) AS avg_disc,
	count(*) AS count_order
FROM
	lineitem
WHERE
	l_shipdate <= DATE '1998-12-01' - INTERVAL '60' day
GROUP BY
	l_returnflag,
	l_linestatus
ORDER BY
	l_returnflag,
	l_linestatus;
```

Postgres `EXPLAIN`:

```
 GroupAggregate  (cost=14.53..18.89 rows=67 width=248)
   Group Key: l_returnflag, l_linestatus
   ->  Sort  (cost=14.53..14.70 rows=67 width=88)
         Sort Key: l_returnflag, l_linestatus
         ->  Seq Scan on lineitem  (cost=0.00..12.50 rows=67 width=88)
               Filter: (l_shipdate <= '1998-10-02 00:00:00'::timestamp without time zone)
(6 rows)
```

Materialize `EXPLAIN`:

```
  Finish order_by=[#0{l_returnflag} asc nulls_last, #1{l_linestatus} asc nulls_last] output=[#0..=#9]
    Project (#0{l_returnflag}..=#5{sum}, #9..=#11, #6{count}) // { arity: 10 }
      Map (bigint_to_numeric(case when (#6{count} = 0) then null else #6{count} end), (#2{sum_l_quantity} / #8), (#3{sum_l_extendedprice} / #8), (#7{sum_l_discount} / #8)) // { arity: 12 }
        Reduce group_by=[#4{l_returnflag}, #5{l_linestatus}] aggregates=[sum(#0{l_quantity}), sum(#1{l_extendedprice}), sum((#1{l_extendedprice} * (1 - #2{l_discount}))), sum(((#1{l_extendedprice} * (1 - #2{l_discount})) * (1 + #3{l_tax}))), count(*), sum(#2{l_discount})] // { arity: 8 }
          Project (#4{l_quantity}..=#9{l_linestatus}) // { arity: 6 }
            Filter (date_to_timestamp(#10{l_shipdate}) <= 1998-10-02 00:00:00) // { arity: 16 }
              ReadIndex on=lineitem pk_lineitem_orderkey_linenumber=[*** full scan ***] // { arity: 16 }

Used Indexes:
  - materialize.public.pk_lineitem_orderkey_linenumber (*** full scan ***)

Target cluster: quickstart
```

New Materialize `EXPLAIN`:

```
  Finish
    Order by: l_returnflag, l_linestatus
    ->  Project (columns=10)
          Columns: l_returnflag..=sum, #9..=#11, count
          -> Map (columns=12)
             (bigint_to_numeric(case when (count = 0) then null else count end), (sum_l_quantity / #8), (sum_l_extendedprice / #8), (sum_l_discount / #8))
             -> Accumulable GroupAggregate (columns=8)
                  Group Key: l_returnflag, l_linestatus
                  Aggregates: sum(l_quantity), sum(l_extendedprice), sum((l_extendedprice * (1 - l_discount))), sum(((l_extendedprice * (1 - l_discount)) * (1 + l_tax))), count(*), sum(l_discount)
                  -> Project (columns=6)
                       Columns: l_quantity..=l_linestatus
                       -> Filter (columns=16)
                            Predicates: date_to_timestamp(l_shipdate) <= 1998-10-02 00:00:00
                            -> Index Scan using pk_lineitem_orderkey_linenumber on lineitem (columns=16)

Used Indexes:
  - materialize.public.pk_lineitem_orderkey_linenumber (*** full scan ***)
```

### TPC-H Query 3

The query:

```sql
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate;
```

Postgres `EXPLAIN`:

```
Sort  (cost=20.78..20.79 rows=1 width=44)
  Sort Key: (sum((lineitem.l_extendedprice * ('1'::numeric - lineitem.l_discount)))) DESC, orders.o_orderdate
  ->  GroupAggregate  (cost=20.74..20.77 rows=1 width=44)
        Group Key: lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority
        ->  Sort  (cost=20.74..20.74 rows=1 width=48)
              Sort Key: lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority
              ->  Nested Loop  (cost=0.29..20.73 rows=1 width=48)
                    ->  Nested Loop  (cost=0.14..19.93 rows=1 width=12)
                          ->  Seq Scan on customer  (cost=0.00..11.75 rows=1 width=4)
                                Filter: (c_mktsegment = 'BUILDING'::bpchar)
                          ->  Index Scan using fk_orders_custkey on orders  (cost=0.14..8.16 rows=1 width=16)
                                Index Cond: (o_custkey = customer.c_custkey)
                                Filter: (o_orderdate < '1995-03-15'::date)
                    ->  Index Scan using fk_lineitem_orderkey on lineitem  (cost=0.14..0.79 rows=1 width=40)
                          Index Cond: (l_orderkey = orders.o_orderkey)
                          Filter: (l_shipdate > '1995-03-15'::date)
(16 rows)
```

Materialize `EXPLAIN`:

```
Finish order_by=[#1{sum} desc nulls_first, #2{o_orderdate} asc nulls_last] output=[#0..=#3]
  Project (#0{o_orderkey}, #3{sum}, #1{o_orderdate}, #2{o_shippriority}) (columns=4)
    Reduce group_by=[#0{o_orderkey}..=#2{o_shippriority}] aggregates=[sum((#3{l_extendedprice} * (1 - #4{l_discount})))] (columns=4)
      Project (#8{o_orderkey}, #12{o_orderdate}, #15{o_shippriority}, #22{l_extendedprice}, #23{l_discount}) (columns=5)
        Filter (#6{c_mktsegment} = "BUILDING") AND (#12{o_orderdate} < 1995-03-15) AND (#27{l_shipdate} > 1995-03-15) (columns=33)
          Join on=(#0{c_custkey} = #9{o_custkey} AND #8{o_orderkey} = #17{l_orderkey}) type=delta (columns=33)
            implementation
              %0:customer » %1:orders[#1]KAif » %2:lineitem[#0]KAif
              %1:orders » %0:customer[#0]KAef » %2:lineitem[#0]KAif
              %2:lineitem » %1:orders[#0]KAif » %0:customer[#0]KAef
            ArrangeBy keys=[[#0{c_custkey}]] (columns=8)
              ReadIndex on=customer pk_customer_custkey=[delta join 1st input (full scan)] (columns=8)
            ArrangeBy keys=[[#0{o_orderkey}], [#1{o_custkey}]] (columns=9)
              ReadIndex on=orders pk_orders_orderkey=[delta join lookup] fk_orders_custkey=[delta join lookup] (columns=9)
            ArrangeBy keys=[[#0{l_orderkey}]] (columns=16)
              ReadIndex on=lineitem fk_lineitem_orderkey=[delta join lookup] (columns=16)

Used Indexes:
  - materialize.public.pk_customer_custkey (delta join 1st input (full scan))
  - materialize.public.pk_orders_orderkey (delta join lookup)
  - materialize.public.fk_orders_custkey (delta join lookup)
  - materialize.public.fk_lineitem_orderkey (delta join lookup)

Target cluster: quickstart
```

New Materialize `EXPLAIN`:

```
Finish
  Order by: sum desc, o_orderdate
  -> Project (columns=4)
       Columns: o_orderkey, sum, o_orderdate, o_shippriority
       -> Reduce (columns=4)
            Group key: o_orderkey..=#2o_shippriority
            Aggregates: sum((l_extendedprice * (1 - l_discount)))
            -> Project (columns=5)
                 Columns: o_orderkey, o_orderdate, o_shippriority, l_extendedprice, l_discount
                 -> Filter (columns=33)
                      Predicates: (c_mktsegment = "BUILDING") AND (o_orderdate < 1995-03-15) AND (l_shipdate > 1995-03-15)
                      -> Delta Join (columns=33)
                           Conditions: c_custkey = o_custkey AND o_orderkey = l_orderkey
                           Pipelines:
                             %0:customer » %1:orders[#1]KAif » %2:lineitem[#0]KAif
                             %1:orders » %0:customer[#0]KAef » %2:lineitem[#0]KAif
                             %2:lineitem » %1:orders[#0]KAif » %0:customer[#0]KAef
                           -> Index Scan using pk_customer_custkey on customer (columns=8)
                                Delta join first input (full scan): pk_customer_custkey
                           -> Index Scan using pk_orders_orderkey, fk_orders_custkey on orders (columns=9)
                                Delta join lookup: pk_orders_orderkey (%1), fk_orders_custkey (%0, %2)
                           -> Index Scan using fk_lineitem_orderkey on lineitem (columns=16)
                                Delta join lookup: fk_lineitem_orderkey (%0, %1, %2)

Used Indexes:
  - materialize.public.pk_customer_custkey (delta join 1st input (full scan))
  - materialize.public.pk_orders_orderkey (delta join lookup)
  - materialize.public.fk_orders_custkey (delta join lookup)
  - materialize.public.fk_lineitem_orderkey (delta join lookup)

Target cluster: quickstart
```

## Alternatives

Should we more radically reduce the AST?

~~Should we abandon static `EXPLAIN` and encourage `mz_lir_mapping`
use?~~ No: being able to `EXPLAIN` ahead of time is valuable, and also
`mz_lir_mapping` forces us to munge strings in SQL.

## Open questions
