# Utility to create arbitrary MIRs

## Limitations

Inherits whatever limitations are in [repr-test-util](../repr-test-util), namely
that support for different types of Datums is incomplete. Follow the
documentation for
[test_spec_to_row](https://dev.materialize.com/api/rust/repr_test_util/fn.test_spec_to_row.html)
to see which types of Datums are currently supported.

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

The `TestCatalog` has following commands.
* `(defsource <name> <RelationType>)` - defines source `<name>` with schema `<RelationType>`

## Unit testing

The tests in this crate are meant to:
1. Test that arbitrary MIR creation works.
2. Give examples for how unit testing with MIRs work.

To create arbitrary MIRs in unit tests of functions on MIRs, import this crate
as a dev dependency to the crate you are testing.
