# Utility for testing lower layers of the Materialize stack

> If you are reading this because you are encountering a compile-time error that
> looks something like:
> ```
>    ::: <path_to_file>:<lineno>:1
>    |
> <lineno> | pub <struct or enum> <Type> {
>    | ------------------- variant or associated item `add_to_reflected_type_info` not found here
>    |
>    = help: items from traits can only be used if the trait is implemented and in scope
>    = note: the following trait defines an item `add_to_reflected_type_info`, perhaps you need to implement it:
>            candidate #1: `MzReflect`
> = note: this error originates in the derive macro `MzReflect` (in Nightly builds, run with -Z macro-backtrace for more info)
>  ```
>
> go [here](#0-required-trait-derivations).

## Purpose

If you want to run tests on something like `MirRelationExpr`, specifying it in
Rust is not only onerous but makes the tests hard to read.

This crate supports converting a specification written in a readable syntax (see
[Syntax](#syntax)) into a Rust object that way you can directly test the lower
layers of the stack. It also supports converting the Rust object back into the
readable syntax.

If your test involves multiple objects, you can still utilize this crate. Just
make all the objects into fields of a single struct. For more information, see
[Test Design](#test-design).

## Syntax

Write:
* an enum as `(variant_in_snake_case field1 field2 .. fieldn)` or
  `(VariantInCamelCase field1 field2 .. fieldn)`.
* a struct with arguments as `(field1 field2 .. fieldn)`
* a struct with no arguments as the empty string.
* a `Vec` as `[elem1 elem2 .. elemn]`
* a tuple as `[elem1 elem2 .. elemn]`
* `None` or `null` as `null`
* `true` (resp. `false`) as `true` (resp. `false`)
* a string as `"something with quotations"`.
* a numeric value like 1.1 or -1 as is.

You can put commas between fields and elements; they will essentially be treated
as whitespace.

The syntax can be overridden and extended by creating an object that implements
the trait `TestDeserializationContext`; see [#extending-the-syntax] for more
details.

### Shortcut for unit enum (resp. a struct with only one argument)
As a convenience, a unit enum (resp. a struct with only one argument) can
alternatively be written as `variant_in_snake_case` (resp. `field1`).

In the case where there are nested single-argument structs, the type resolution
system always tries to resolve your specification as an instance of the current
type before trying to resolve the specification as the type of the single
argument.

Suppose you had two structs:
```
struct InnerStruct {only_arg: Option<u32>}
struct OuterStruct {only_arg: Option<InnerStruct>}
```
and you were trying to create an instance of `OuterStruct`. Specifiying:
* `null` or `(null)` result in an `OuterStruct{only_arg: None}`.
* `((null))` results in an `OuterStruct{only_arg: Some(InnerStruct{only_arg: None})}`.
* `1`, `(1)`, or `((1))` result in `OuterStruct{only_arg: Some(InnerStruct{only_arg:Some(1)})}`.

If you have a struct whose only argument is a non-unit enum (resp. a struct
with multiple arguments), you should use two sets of parentheses, e.g.
`((variant_in_snake_case field1 .. fieldn))` (resp. `((field1 .. fieldn))`) to
make it clear that the multiple arguments belong to the inner enum (resp.
struct) and not the outer struct.

Just as with regular enums, `variant_in_snake_case` can be replaced with `VariantInCamelCase`.

### Shortcut for `String`

A object of type `String` can be optionally specified without quotations if the
string is something other than `"null"`.

## Creating an object

Look in the [tests](./tests) folder of this crate for an example.

The main object creation method is
```
pub fn deserialize<D, I, C>(
    stream_iter: &mut I,
    type_name: &'static str,
    ctx: &mut C,
) -> Result<D, String>
where
    C: TestDeserializeContext,
    D: DeserializeOwned + MzReflect,
    I: Iterator<Item = proc_macro2::TokenTree>
```

Let's break down what you need to do before you can call `deserialize` and what you
should supply to `deserialize`.

### 0. Required trait derivations

The methods in this crate translate the test syntax into a JSON formatted string
that can be correctly deserialized by [serde_json]. Because of this, the object
you are trying to create must implement `serde::Deserialize`. It is
recommended that the object also implement `serde::Serialize`.

In order to translate the text syntax into a correctly-deserializable JSON
string, we need information about enums and structs being created that are not
specified in the test syntax.

Thus, you need to derive the trait `MzReflect` for each enum or struct that
you will need to construct your object. If you cannot derive the enum or struct
because it is from an external crate, then you should add its name to
`EXTERNAL_TYPES` in
[../lowertest-derive/src/lib.rs](../lowertest-derive/src/lib.rs).

If you have added a new type that is relied on by a type that derives the trait
`MzReflect` and you do not derive the trait `MzReflect` or add the name of the
type to `EXTERNAL_TYPES`, you may encounter the compile-time error shown at the
top of this README.

Default values are supported as long as the default fields are last. Put
`#[serde(default)]` over any fields you want to be default and make sure those
fields have default values.

You can add the attribute `#[mzreflect(ignore)]` to a field to avoid having to
derive `MzReflect` as long as at least one of the following conditions apply:
* you have [overridden the syntax](#extending-the-syntax) such that you can
construct an instance of the field without using the DSL
* you have marked the field with the attribute `#[serde(default)]` and always
  want the field to be initialized with the default value.

[serde_json]: https://docs.serde.rs/serde_json/

### 1. Parsing the test syntax

Tokenize the string `s` containing your specification by calling
`let mut stream_iter = lowertest::tokenize(s)?.into_iter()`.

You now have an iterator over [`TokenTree`][proc_macro2] that you can pass
into `deserialize`.

You can specify the creation of multiple objects in the same string and
deserialize them in order by making repeat calls to `deserialize`, passing in
the same mutable iterator reference each time.

### 2. Supplying the name of the object's type

You have to pass in a string containing the name of your type.

Rust does not have type objects that you can manipulate. Thus, information about
enums and structs are keyed by string.

### 3. Feeding in syntax extensions

If you don't need to extend or override the syntax, give a
`&mut GenericTestDeserializationContext::default()`.

Otherwise, see ["Extending the syntax"](#extending-the-syntax).

## Extending the syntax

Create an object that implements the trait `TestDeserializationContext`.

The trait has a method `override_syntax`.
* Its first argument is `&mut self` this way the `TestDeserializationContext`
  can store state across multiple objects being created.
* The second and third arguments of the method is the first element in the
  stream and a pointer to the rest of the stream respectively. The contract of
  `override_syntax` is that you promise not to iterate through the stream more
  than is necessary to construct your object. If you return `Ok(None)`, you
  should have left the stream untouched.
* The fourth argument is the type of the object whose specification is currently
  being translated. Note that the name of the type is only guaranteed to be
  accurate if it is a [supported type](#supported-types). Thus, if an enum
  variant (resp. struct) you are trying to create has a field that is not a
  supported type, you should use `override_syntax` to specify how to create the
  enum variant (resp. struct).
* The return value should always be `Ok(None)` except in the specific cases when
  you are overriding or extending the syntax, in which case the return value
  will be either `Ok(Some(JSON_string))` or `Err(err_string)`.

Refer to the [proc_macro2] docs for information how to parse the stream.

[proc_macro2]: https://docs.rs/proc-macro2/1.0.27/proc_macro2/enum.TokenTree.html

If an object being created is an enum or struct that derives `MzReflect`, what
is passed to `override_syntax` changes:
* If the spec is a parentheses group (<arg1> .. <argn>), `first_arg` will be
  `<arg1>` and `rest_of_stream` will be `<arg2> .. <argn>`. This saves you the
  effort of unpacking the parentheses group.
* If the spec is an ident or literal, `first_arg` will be the ident or literal,
  and `rest_of_stream` will be empty.
* If the spec is an punct, `first_arg` will be the punct, and `rest_of_stream`
  will be `<any_puncts_in_between> <first_not_punct_token>`.

Since a one-arg enum or struct can be specified as `(<the_arg>)` or
`<the_arg>`, passing in the specification in the manner described above saves
you from having to implement both syntaxes in `override_syntax`.

The trait has a second method `reverse_syntax_override` to convert from JSON
to the extended syntax. See [Roundtrip](#roundtrip) for more details.

If you implement the ability to construct an enum variant or a struct with
an alternate syntax and you believe:
1. the default syntax should not be used. OR
2. it is unnecessary for the default syntax to be supported.

you can add the attribute `#[mzreflect(ignore)]` to one or more fields of the
enum variant or struct so that their fields and any subtypes do not need to
derive `MzReflect`. This has the side effect of disabling the ability to
construct the enum variant or the struct using the default syntax.

### Supported types

* Enums
* Structs
* Primitive types
* Strings
* Tuples
* `Box<>`
* `Option<>`
* `Vec<>`

Generally, combinations of the above are supported. The exception are
types with values that `serde_json` cannot distinguish from `None` because
`serde_json` serializes them all to the same string "null".
* Multiple `Option<>`s nested within each other, e.g. `Option<Option<u32>>`, or
  `Option<SingleUnnamedOptionStruct>`, where the struct is defined as
  `SingleUnnamedOptionStruct(Option<u32>)`. `serde_json` cannot distinguish
  `Some(None)` from `None`.
* `Option<NoArgumentStruct>`. `serde_json` cannot distinguish
  `Some(NoArgumentStruct)` from `None`.

The type `Result<<ok_type>, <error_type>>` is not yet supported.

## Roundtrip

To convert a Rust object back into the readable syntax:
1) Serialize the Rust object to a [serde_json::Value] object by calling
   `let json = serde_json::to_value(obj)?;`.
2) Feed the json into the method `serialize::<T, _>`. `T` is the type of the
   object that the json represents.

[serde_json::Value]: https://docs.serde.rs/serde_json/enum.Value.html

## Test Design

Suppose you want to test `funcX`, a function with `n` arguments.

Instead of trying to construct or deserialize each of the `n` arguments one by
one, you could define a struct with the `n` arguments as fields so that you can
deserialize all the arguments in one go.
```
struct FuncXParams {
    arg1: ...
    arg2: ...
    ...
    argn: ...
}
```

Likewise, if you want to run arbitrary sequences of functions selected from a
finite set `{Func1, ..., Funcj}`, you can define a enum where each variant
represents a different function and its arguments.
```
enum PotentialFunc {
    Func1{arg1: ..., ..., argi: ... }
    ...
    Funcj{arg1: ..., ..., argn: ...}
}
```
You can either:
* express the sequence of functions as a `Vec<PotentialFunc>`
* have a `while` loop that parses the sequence of functions by calling
  `deserialize_optional::<PotentialFunc, _, _>` repeatedly on your input string.
  Using the `while` loop allows you to not have `[]` around the sequence of
  functions.
