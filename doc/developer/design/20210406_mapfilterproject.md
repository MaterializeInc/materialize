# MapFilterProject Pushdown

## Summary

<!--
// Brief, high-level overview. A few sentences long.
// Be sure to capture the customer impact - framing this as a release note may be useful.
-->

Dataflow computations consume data from sources, but rarely require all of the information that a source can provide.
Some simple transformation, filtering, and projection can be applied in-place on the data, as (or even before) it arrives to the dataflow.
This proposal presents one way of wrapping and communicating this information to sources, who can then implement it if appropriate.

## Goals

<!--
// Enumerate the concrete goals that are in scope for the project.
-->

The main goal is to define a concrete encapsulation of operations a source can apply to its stream of records before presenting them to dataflow.
In addition, we want to assess the fitness for use of this encapsulation, to determine that it is (or is not) helpful for sources.
We intend to frame an interface with sources that allow them to communicate back to dataflow what of the operations they were able to perform.

## Non-Goals

<!--
// Enumerate potential goals that are explicitly out of scope for the project
// ie. what could we do or what do we want to do in the future - but are not doing now
-->

We do not intend to determine the policy for which operators should be communicated to sources and which should be held back.
We expect a substantial amount of follow-up work on source implementations where we determine how best to apply these operators.
We do not intend to communicate expressions over things other than `[Datum]` input (e.g. we are not directly expression JSON or Avro navigation).
We could go further with the set of operations that we push in to sources, including e.g. `FlatMap`, `Negate`, or `Union` operators.

## Description

<!--
// Describe the approach in detail. If there is no clear frontrunner, feel free to list all approaches in alternatives.
// If applicable, be sure to call out any new testing/validation that will be required
-->

We chose the name `MapFilterProject` to represent the idea that we are capturing a sequence of `Map`, then `Filter`, then `Project` statements.

The `MirRelationExpr::Map` operator contains a sequence of `MirScalarExpr` expressions.
Each expression is a function that is applied to the existing columns, and which produces a new column.
The sequence of expressions indicates a sequence of new columns to add to each row input to the `Map`.

The `MirRelationExpr::Filter` operator contains a sequence of `MirScalarExpr` expressions.
Each expression is a function that is applied to the existing columns, and which produces either `Datum::True` or not.
Rows that produce `Datum::True` for all expressions are retained in the output, and rows that produce `Datum::False` or `Datum::Null` for any expressions are discarded.
The production of any other `Datum` variant is a type error.

The `MirRelationExpr::Project` operator contains a sequence of integer column references.
Each column reference indicates that the value found in that column should be copied to the output in place of the reference.
The projection allows us to remove various columns from the input, and re-order and duplicate existing columns.

These three operations can by applied row-by-row, as their actions have no relation to other rows in the system.
Arbitrary sequences of these operations can always be reduced to one `Map`, then one `Filter`, then one `Project`.

The proposal is to assemble and communicate this information (caveat: some modifications will be made) to sources:
```rust
pub struct MapFilterProject {
    /// A sequence of expressions that should be appended to the row.
    pub expressions: Vec<MirScalarExpr>,
    /// Expressions that must evaluate to `Datum::True` for the output
    /// row to be produced.
    pub predicates: Vec<MirScalarExpr>,
    /// A sequence of column identifiers whose data form the output row.
    pub projection: Vec<usize>,
}
```
The three members correspond exactly to one `Map`, then one `Filter`, then one `Project`.

On instantiation each source would be supplied with a `&mut MapFilterProject` argument.
Depending on the capabilities of the source, it could take any or all of the operations, and leave behind any work undone.
A simple default would be to leave the argument untouched, communicating that it applied none of the operations.
A thorough source could take the entire instance and leave behind an identity operator, communicating that there is no remaining work to be done.

### Manipulation

The `MapFilterProject` structure has several methods that are meant to facilitate its manipulation.
For example, the `demand(&self)` method indicates which input columns will be examined by the instance.
Additionally, the `permute(&mut self, ..)` method re-writes all column references as if the input had been permuted.
These two together allow sources to determine which of their announced columns need to be constructed at all, and to rewrite a `MapFilterProject` instance to reference only those columns that were constructed.

As an example, the `csv.rs` source provides the appearance of multiple columns of text.
A `MapFilterProject` instance may only consult three of those columns, and ignore all of the others.
By default, the `csv.rs` source will form a `Datum::String` for each of the presented columns.
The source implementation can
1. use `mfp.demand()` to determine which columns are required by the `mfp` instance,
2. use `mfp.permute()` to change the column references to point to a new, dense set of column identifiers, and then
3. only assemble values for those referenced columns and apply `mfp` to the result.

This idiom is especially valuable in situations where input columns may be more complicated, for example Avro metadata, which can involve expensive decoding to prepare, but which can be entirely skipped if none of the contents are referenced.

In the worst case, a `MapFilterProject` instance can be converted to a sequence of `Map`, `Filter`, and `Project` expressions with the `as_map_filter_project()` method.
The resulting expressions can be manipulated and reassembled by users who need more precise control.

For a more challenging example, navigation of JSON-structured data presents as field accesses.
When selecting nested fields from such a structure,
```
SELECT
    a::json->'Field1'->'Foo',
    a::json->'Field1'->'Bar',
    a::json->'Field2'->'Baz',
    a::json->'Field2'->'Quux'->'Zorb'
FROM x
```
The resulting `Map` and `Project` plans encode the navigation:
```
| Get materialize.public.x (u1)
| Map strtojsonb(#0),
|     (#2 -> "Field1"),
|     (#3 -> "Foo"),
|     (#3 -> "Bar"),
|     (#2 -> "Field2"),
|     (#6 -> "Baz"),
|     ((#6 -> "Quux") -> "Zorb"),
| Project (#4, #5, #7, #8)
```
The `MapFilterProject` instance wuold contain the `Map` and `Project` instructions, passed to what might be a source `x`.
The expressions themselves do not themselves provide efficient navigation, but a sufficiently smart JSON navigator could follow them and avoid irrelevant fields.

### Planning for evaluation

The `MapFilterProject` structure contains information about the logical operations, but it is not itself evaluable on data.
Instead, it provides an `into_plan(self)` method which yields an evaluable `MfpPlan` instance.
This means to separate out the information about what to do from the machinery to do it (for reasons that occur in the next section).

The plan instances have a method `evaluate(&self, ..) -> Row` which take as an argument a `&mut Vec<Datum>`.
These are the decoded columns presented in a buffer than can be appended to.
The use of decoded columns makes it relatively cheap for sources to install simple data types (e.g. integers, booleans) without `Row` packing or unpacking.

There are a variety of `evaluate_*` methods, which do various amounts of the evaluation work.
Some instances will pack the results in a `Row`, others produce an iterator over the columns.
There is space to negotiate here, but the intent is that the `MfpPlan` instance is capable of handling the execution for the user.

### Temporal Filters

One caveat to all of this is that we have predicates that cannot be "evaluated" in the standard sense.
Specifically, we have "temporal predicates" that relate logical time to functions of row data.
These predicates do not evaluate to `Datum::{True, False, *}` and instead require some care in evaluation.

The `MfpPlan` mentioned above may contain temporal operators, and its `evaluate` method handles their evaluation.
Its signature is a bit more complicated, because it may result in multiple outputs at different logical times, with different signs.
However, we can see at plan-time if the plan contains temporal operators, and simplify it to a `SafeMfpPlan` type if it does not.
The `MapFilterProject` type also has an `extract_temporal(&mut self) -> Self` operator that extracts all temporal predicates, partitioning the work into non-temporal and temporal for sources that only want to handle the former.

Generally, temporal filters are not hard to apply in-line, but they do have the potential to produce more than one output record for each input record.
This makes them very ill-suited for some sources like `Upsert` that are memory-sensitive.
This operator can apply temporal filters, but it wants to partition the work into non-temporal (and non-expanding) operations, followed by the temporal (and potentially expanding) actions only when it is ready to emit output.

Naive sources should feel comfortable using `extract_temporal()` to leave themselves with non-temporal operators and leave the rest of the work for someone else to do.

## Alternatives

<!--
// Similar to the Description section. List of alternative approaches considered, pros/cons or why they were not chosen
-->

The current approach is the `LinearOperator` structure.

This is a combination of `Filter` expressions and "demand", which is roughly analogous to `Project` (it allows certain columns to be filled with `Datum::Dummy` as we know they will not be read).

This operator is mainly ad-hoc and not fully expressive.
It cannot represent `Map` expressions, and so cannot represent common sub-expression optimizations.
It also cannot represent tasks like decoding `text` to `json` and then discarding `text`, as a `Map` and `Project` can do.

The intent was good here, but `MapFilterProject` is meant to be the grown-up version of this.

Other options include passing Rust closures to sources, though this lacks the flexibility of `MapFilterProject`.
In particular, we would only gain the optimization benefits if the closure were known at compile-time.
But, to support arbitrary expressions, predicates, and projections, we need these to be determined as a function of queries, after compilation.

## Open questions

<!--
// Anything currently unanswered that needs specific focus. This section may be expanded during the doc meeting as
// other unknowns are pointed out.
// These questions may be technical, product, or anything in-between.
-->

What interface elements are necessary and sufficent for `MapFilterProject` and `MfpPlan`?
For example, to navigate an Avro struct the `expressions` contain a sequence of navigation expressions;
will we need to present these expressions (immutably) for optimal use? how would a smart Avro source use this information?

How ergonomic are the `MfpPlan` and `SafeMfpPlan` methods for evaluation?
There are currently various quirks related to temporary allocation, evaluation errors that may occur, and other nuances;
can these be shaped in a way that prevent mis-use and generally don't require tens of lines of code around each call?
