# Optimizations

## Testing

The tests in this crate inherit all TestCatalog commands from the
[expr-test-util](../expr-test-util) crate.

There are five types of transform tests:
* `build apply=(transform1,..,transformn)`. `Transform1` through `TransformN`
  get applied to the input MirRelationExpr in the order specified.
* `opt`. The full set of transformations are applied to the input
  MirRelationExpr.
* `steps`. The full set of transformations are applied to the input
  MirRelationExpr. The output includes each transformation that changed the
  MirRelationExpr and what the MirRelationExpr right after that transformation.
* `crossview apply=(transform1,..,transformn)` `Transform1` through `TransformN`
  get applied in the order specified to the sequence of interdependent views.
  Views should be listed in order of how they would be rendered.
* `crossviewopt` The full set of transformations are applied to the input
  sequence of interdependent views. Views should be listed in order of how
  they would be rendered.

For all test commands, the input is expected to be a specification for a
MirRelationExpr. By default the input syntax is the one described in the
[expr-test-util](../expr-test-util/README.md#syntax) crate. For single-view
tests, you can add the option `in=json` to change the input syntax to the
`serde_json` version of the MirRelationExpr.

By default, the output is expressed in the same form as `EXPLAIN <query>`.
Alternate output formats:
* `format=types` will express the output in the same form as `EXPLAIN TYPED PLAN FOR <query>`.
* `format=json` will serialize the output MirRelationExpr using `serde_json`.
* `format=test` will express the output MirRelationExpr in the
  [expr-test-util syntax](../expr-test-util/README.md#syntax). It will append to
  the end of the output catalog commands required to register in the test
  catalog sources not yet registered.
  > Note that this output format does not just yet support scalar subqueries.

## Debugging

To figure out why a particular optimized plan was generated for a particular SQL
query, you may want to extract the MirRelationExpr at some point during the
planning process and then run some tests to experiment with how transformations
affect the MIR, to figure out what are the minimal elements that an MIR should
have to reproduce the behavior, etc. etc.

1. Add `println!("{}", <MIR variable name>.json())` to the place in the code of
   your choosing so it will print out your MIR serialized into a JSON string.
2. Compile Materialize and run your SQL query.
3. Copy the printed JSON string into a file in [tests/testdata]. Add the test
   command of your choice (most likely `build` or `steps`). Be sure to include
   the argument `in=json`. If you want to use the output of your test in further
   tests, add the argument `format=test` or `format=json`.
4. Run `REWRITE=1 cargo test` from this directory. The test output will be
   written to the file right below your test.

Refer to [tests/testdata/steps] as an example.
