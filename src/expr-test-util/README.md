# Utility to create arbitrary MIRs

## Syntax

See the README for the [lowertest](../lowertest/README.md) crate for the
standard syntax.

The following variants of `MirScalarExpr` have non-standard syntax:
1. Literal -> the syntax is `(<literal> <scalar type>)` or `<literal>`.
    If `<scalar type>` is not specified, then literals will be assigned
    default types:
   * true/false become Bool
   * numbers become Int64
   * strings become String
2. Column -> the syntax is `#n`, where n is the column number.

The following variants of `MirRelationExpr` have non-standard syntax:
1. Let -> the syntax is `(let x <value> <body>)` where x is an ident that
    should not match any existing ident in any Let statement in
    `<value>`.
2. Get -> the syntax is `(get x)`, where x is an ident that refers to a
    pre-defined source or an ident defined in a let.
3. Union -> the syntax is `(union <input1> .. <inputn>)`.
4. Constant -> the syntax is
    ```
    (constant
    [[<row1literal1>..<row1literaln>]..[<rowiliteral1>..<rowiliteraln>]]
    <RelationType>
    )
    ```

## Unit testing

The tests in this crate are meant to:
1. Test that arbitrary MIR creation works.
2. Give examples for how unit testing with MIRs work.

To create arbitrary MIRs in unit tests of functions on MIRs, import this crate
as a dev dependency to the crate you are testing.

## Debugging

To figure out why a particular optimized plan was generated for a particular SQL
query, you may want to extract the MirRelationExpr at some point during the
planning process and then run some tests to experiment with how transformations
affect the MIR, to figure out what are the minimal elements that an MIR should
have to reproduce the behavior, etc. etc.

1. Add `println!("{}", <MIR variable name>.json())` to the place in the code of
   your choosing so it will print out your MIR serialized into a JSON string.
2. Run your SQL query.

At this point, you could just copy the JSON string into a test of your choice
and convert the JSON string back into a `MirRelationExpr` with the line
`let variable: MirRelationExpr = serde_json::from_str(<the_json_string>)?;`

But if you want to convert the JSON string into the more readable test syntax, follow these steps:

1. Copy the JSON string to a file that looks like this:
   ```
   rel-to-spec
   <JSON version of your MIR>
   ----
   ```
2. In the line in [tests/test_runner.rs] beginning with
   `datadriven::walk("tests/testdata"`, change `"tests/testdata"` to point to
   the file created in the previous step.
3. On the command line, run `cd <this directory>; REWRITE=1 cargo test`. Your
   file should be rewritten so it looks like this:
   ```
   rel-to-spec
   <JSON version of your MIR>
   ----
   ----
   <commands to register in the test catalog the sources that your MIR references>

   <specification to create your MIR in unit tests>
   ----
   ----
   ```
