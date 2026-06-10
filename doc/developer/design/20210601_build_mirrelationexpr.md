# More easily build lower level IRs

## Summary

The dataflow and optimization layers are hard to test. Currently the way you
test most of the code is that you have to come up with a SQL query will
result in the `MirRelationExpr` you want.

A blocker to more directly testing these two layers is that specifying a
particular `Mir{Relation, Scalar}Expr` in Rust is onerous. Even if you put the
effort in to specify something like this code below, the test becomes difficult
to read.
```
let mut test_expr = MirRelationExpr::Filter {
    input: Box::new(MirRelationExpr::FlatMap {
        input: Box::new(MirRelationExpr::Map {
            input: Box::new(MirRelationExpr::Get {
                id: Id::Global(GlobalId::User(0)),
                typ: RelationType::new(vec![
                    ColumnType {
                        nullable: true,
                        scalar_type: ScalarType::Int32,
                    },
                    ColumnType {
                        nullable: true,
                        scalar_type: ScalarType::Int64,
                    },
                ]),
            }),
        scalars: vec![MirScalarExpr::literal_null(ScalarType::Int32)],
        }),
        func: TableFunc::GenerateSeriesInt32,
        exprs: vec![MirScalarExpr::Column(1)],
        demand: None,
    }),
    predicates: vec![MirScalarExpr::CallBinary {
        func: BinaryFunc::Eq,
        expr1: Box::new(MirScalarExpr::Column(0)),
        expr2: Box::new(MirScalarExpr::Column(3)),
    }],
};
```

Justin had started on a system, which can be found at
[/src/transform/tests](/src/transform/tests)
where you can specify a `Mir{Relation, Scalar}Expr` using a much more readable
and simple syntax:
```
(
  filter
  (
    flat-map
    (map (get x) [(null int32)])
    generate-series-int32
    [#1]
  )
  [(call-binary eq #0 #3)]
)
```

but implementing the syntax itself is onerous because Rust can't take a string
and just convert it to an enum. The code for constructing an `BinaryFunc`
would look like:

```
match func {
"eq" => BinaryFunc::Eq
"isregexpmatch" => {
    let case_insensitive = self.next_sexp().parse::<bool>();
    BinaryFunc::IsRegexpMatch {case_insensitive}
}
<repeat for every single BinaryFunc variant>
}
```

As such, currently Justin's system only supports the following flavors:
* Of `MirScalarExpr` => columns, numeric literals, bool literals
* Of `MirRelationExpr` => get, let, map, filter, project, constant, join (only
`JoinImplementation::Unimplemented` ), union, negate, arrange-by

Figuring out how to more efficiently implement the syntax has been an ongoing
issue. There has been some earlier design discussions on this, which are
documented in the comments of [#5684].

[#5684]: https://github.com/MaterializeInc/database-issues/issues/1750

## Goals

* Allow the easy construction of all, or at
  least the vast majority of, `Mir{Relation, Scalar}Expr`. Being able to do this
  will help a lot with:
  * Optimization/transform unit testing.
  * Testing the dataflow layer directly. Currently, there are some branches
    that exist so Materialize does not crash if there are optimizer bugs. These
    branches end up untested because we have eliminated all known optimizer
    bugs that would trigger those branches.
  * Experimentation. Being able to construct arbitrary `Mir`s can allow for
    testing the effects of proposed transforms without having to write code for
    the transform.
  * Debugging. Often times you want to find a simpler reproduction for a
    complicated query that is failing, but changing the SQL may result in a
    complete different `Mir` representation.
* Create a system that can easily ported to supporting the easy construction
  of other complex internal expressions so that we don't have to go through
  this again if we want to test or play around with other stuff. Example other
  internal expressions:
  * `ReducePlan` (a.k.a `LIRR`, "LIR for Reduce")
  * future `Lir` expressions
  * possibly `Hir` expressions? We have been talking about doing some rewrites before the decorrelation stage.
  * `DataflowDescs` to test optimization across dataflows?

## Non-Goals

* The consensus from the earlier discussion is that the
  `Mir{Relation, Scalar}Expr` builder should not bother with attempting to
  infer the type of a column. E.g. when doing a sum reduce, it is up to the
  test-writer to select the correct `AggregateFunc` from `SumInt32`,
  `SumInt64`, etc.

## Description

### General Approach

Let's call this approach "Minimal boilerplate".

I propose to retain Justin's syntax and invest in a mechanism that can
automatically convert it in to a JSON formatted string that can be correctly
deserialized by [serde_json].

The mechanism has 3 components:
(1) A parser. See the section [Parser changes](#parser-changes) for more
details.

(2) A derive macro that generates, for each enum, a mapping from the enum's
variants to the names and the types of the variant's fields.

(3) Code that constructs the JSON formatted string, stitching the parsed tokens
to the information about the enums.

Pseudocode for (3) looks like this:
To construct the JSON string for an enum of type `type` specified as
`(variant <arg1> .. <argn>)`:
1. If the context passed in determines that `(type, variant)` combination
   requires special action, take it.
   Otherwise, construct the JSON string in the default manner, which is:
2. Look up the names and the types of the fields of `Type::Variant`
3. Construct the JSON strings for the objects corresponding to `<argi>`
   based on the type information.
4. The JSON string, if the variant has named fields, looks like
   `{"Variant": {"Arg1Name": <arg1_json>, ... "ArgNName": <argn_json>}}`.
   If the variant has unnamed fields, it looks like
   `{"Variant": [<arg1_json>, .., <argn_json>]}`. If the variant has no fields,
   it looks like `"Variant"`.

(3) supports optional arguments. For example, if you have the spec
`(join <input> <equivalences>)`, the corresponding JSON string
```
{"Join": {"input": <input>, "equivalences": <equivalences>}}
```
will deserialize fine into a `MirRelationExpr::Join` as long as you add
the attribute `#[serde(default)]` to the definition of the optional fields
`implementation` and `demand`.

Step 1 in the pseudocode enables support for irregular syntax. For example, the
syntax `(null int32)` corresponds to the `MirScalarExpr` null literal, which has
a corresponding JSON string
`{"Literal":[{"Ok":{"data":[0]}},{"nullable":true,"scalar_type":"Int32"}]}`.
In this case, `MirScalarExpr` `null`
with first argument `null` and goes to a separate branch to construct a
`MirScalarExpr::Literal`. The `MirScalarExpr::Literal` gets re-serialized into
JSON and pasted into the string for the map.

While I have been focusing on describing how to convert Justin's syntax into a
string that [serde_json] will correctly deserialize as an enum, it should be
easy to see how the mechanism can support converting the syntax into a string
that [serde_json] will correctly deserialize as a struct.

[serde_json]: https://docs.serde.rs/serde_json/

### Parser changes

I propose replacing the Justin's existing sexp [parser] with
[proc_macro2::TokenStream].

The primary motivation is "why write your own parser when you don't have to?"

Also, I like how despite its name, `proc_macro2::TokenStream` turns the syntax
into a tree that mirrors the structure of the tree that we're trying to build.

For example, `proc_macro2::TokenStream` parses into
`(map (get x) [(null int32)])` into something like:
```
ParenGroup [0]         [1]                           [2]
            |           |                             |
            V           V                             V
            Ident(map)  ParenGroup [0] [1]  BracketGroup [0]
                                    |   |            |
                                    V   V            V
                            Ident(get)  Ident(x)     ParenGroup [0] [1]
                                                                 |   |
                                                                 V   V
                                                       Ident(null)  Ident(int32)
```

A consequence of doing this replacement is that idents in Justin's original
syntax that are in kebab-case need to be changed to snake_case because the
parser will recognize `stuff_in_snake_case` as one ident but will think
`stuff-in-kebab-case` is four different idents separated by '-'.

[parser]: https://github.com/MaterializeInc/materialize/blob/9188ac95ffc03b598b1118e4e88df6867e4b718e/src/transform/tests/test_runner.rs#L22
[proc_macro2::TokenStream]: https://docs.rs/proc-macro2/1.0.27/proc_macro2/struct.TokenStream.html

### Follow-up: Round Trip Support

Creating a reversal mechanism, a.k.a. converting the JSON string serialization
back to Justin's syntax, is not a priority compared to enabling
testing/experimentation/debugging. I have a
hard time thinking of a use for round-trip support other than test migration
(see below).

That said, the reverse mechanism works just like the forward mechanism, with the
exception that the parser for the reverse mechanism is the [json] crate. In
order to translate substrings back to irregular syntax, it
will use the same derive macro to track the type substrings correspond to.

[json]: https://docs.rs/json/0.12.4/json/

### Follow-up: Test Migration Support

If we have round-trip support, then it becomes easy to support migrating tests
if the structure of an class of expression changes.

For example, if `MirRelationExpr::{Map, Filter, Project}` were being replaced by `MirRelationExpr::MapFilterProject`, you would

1. Add the variant `MirRelationExpr::MapFilterProject`.
2. Write a transform replacing `MirRelationExpr::{Map, Filter, Project}` with
   `MirRelationExpr::MapFilterProject`.
3. For each test spec,
   1. Construct the corresponding `MirRelationExpr`.
   2. Run the transform.
   3. Replace the original test spec with the spec of the transformed
   `MirRelationExpr`.
4. Delete `MirRelationExpr::{Map, Filter, Project}`

To reduce the amount of work required to migrate tests, I propose creating a
template for step (3) when there is available bandwidth or when doing a test
migration for the first time. The template can do all the
test rewrites, and one just has to supply the transform.

## Alternatives

### To the general approach

#### Strum

This approach was rejected in the earlier discussion, but I am listing it for
the sake of completeness.

The idea was to use the `strum` crate to [derive a method that will construct the
enum from a string](https://docs.rs/strum/0.21.0/strum/derive.EnumString.html).

The original objection to this approach is concern that the `strum` crate is
insufficiently mature to depend on.

It should also be noted that for enum variants with fields, the macro will
populate the fields with default values, so:
1.  the macro will not work for enums with `Box` fields like
    `MirRelationExpr::Map`.
2.  even if the enum does not have a `Box` field, it will take extra
    boilerplate code to enable specifiying an enum with non-default values.

#### All deserialization

This proposal may bring up the question of "Why use Justin's syntax instead of
just specifying all expressions using JSON strings?"

There are two reasons:
* JSON strings result in longer specifications because you have to include the
  field names.
* More importantly, some enum variants, especially `MirRelationExpr::Constant`, are
  very hard to specify as JSON strings that will deserialize properly. Since we
  would need some kind of parsing mechanism to convert part of the spec into
  stuff like `MirRelationExpr::Constant` anyway, we might as well use the
  parsing mechanism to enable specifying the whole thing in a simpler syntax.

#### All macro/Custom serde implementation

There is also the question of, "Why construct a JSON string and then deserialize
that instead of creating a macro generates a method that directly converts
Justin's syntax to the expression?"

My belief is that macros are hard to write, introspect, and debug. Using
`serde_json` gets string to enum conversion with a lot less coding effort and
with more insight on where you may have made a mistake in your syntax if the
deserialization fails.

#### All boilerplate

A.k.a. Just grind through all the enums and enum variants, filling in the
entirety of methods of like
```
match func {
"eq" => BinaryFunc::Eq
"isregexpmatch" => {
    let case_insensitive = self.next_sexp().parse::<bool>();
    BinaryFunc::IsRegexpMatch {case_insensitive}
}
<repeat for every single BinaryFunc variant>
}
```

This is essentially the current state of unit testing with
`Mir{Relation, Scalar}Expr`.

Pros: No macros required. No major syntax change.

Cons: There are a lot of enums and enum variants. This is especially the case
for enums whose type names end in `Func`. Also, new variants for enums
whose type names end in `Func` are ever-increasing, and these variants are being
added by different team members from those who are interested in constructing
arbitrary `Mir{Relation, Scalar}Expr`.

The status quo is that we attempt to amortize the time costs of putting in
boilerplate by adding boilerplate as test cases need. I feel that there are
a couple of problems with the status quo:
* We get discouraged from trying to create `Mir{Relation, Scalar}Expr` for the
  purposes of debugging/experimentation/testing the dataflow layer directly
  because of the amount of boilerplate required to get started.
* When we add unit tests to test a new feature, we are incentivized to add
  tests that require adding the minimum amount of new boilerplate, which makes
  the test coverage less good.

#### Hybrid boilerplate/deserialization

The idea here is to take advantage of the fact that while enums whose names end
with `Func` have an ever-increasing number of variants, the number of variants
for enums whose names end in `Expr` are pretty stable.

Keep Justin's syntax for `MirRelationExpr` and `MirScalarExpr` and implement
Justin's syntax with boilerplate. But specify `*Func` enums using JSON strings
can be correctly deserialized using [serde_json].

Most `*Func` variants are unit variants, and you can get the unit enum variant
of your choice by using [serde_json] to deserialize the name.
```
serde_json::from_str::<TableFunc>("GenerateSeriesInt32")
```

Most of the `*Func` variants that are not unit variants have only one
argument, so while specifying those variants using a JSON string involves
making the syntax more verbose, it doesn't look all that bad. Examples:
* `(flat_map (get x) {"CsvExtract":2} [#0])`
* `(flat_map (get x) {"JsonbArrayElements": {"stringify": true}} [#0])`

There are a few worst-case variants where the JSON string can be rather
onerous to specify, for example `TableFunc::RegexpExtract`:
```
{"RegexpExtract":
   [
        <the regex>,
        [{"index":0, "name": "something", "nullable": false} ... {..}]]
   ]
}
```
which we can address by either:
* hoping that people will only rarely want to construct expressions with difficult
  functions.
* adding boilerplate for constructing the worst-case variants.

This approach requires no macros. It involves less boilerplate required than the
"All boilerplate" option. It involves more boilerplate than the "Minimal
boilerplate" option. If we just want to use Justin's syntax to construct
`Mir{Relation, Scalar}Expr`s, the difference in boilerplate will not be that
much, but the difference will be more noticeable if we start wanting to use the
syntax to construct other things.
