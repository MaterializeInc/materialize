# Utility for testing lower layers of the Materialize stack

## Purpose

If you want to run tests on something like `MirRelationExpr`, specifying it in
Rust is not only onerous but makes the tests hard to read.

This crate supports converting a specification written in a readable syntax (see
[#syntax]) into a Rust object that way you can directly test the lower layers of
the stack. It also supports converting the Rust object back into the readable
syntax.

Note that this crate can help create not only a Rust object to run tests on but
also the test itself. Suppose you want to run `funcX`, a function with `n`
arguments. Instead of initializing each of the `n` arguments one by one in Rust
code, you can define a struct or enum with those `n` arguments as fields and use
this crate to convert a test specification into an instance of the struct or
enum. Then, you can feed the fields of the struct into `funcX`.

## Syntax

Write:
* an enum as `(variant_in_snake_case field1 field2 .. fieldn)` or
  `(VariantInCamelCase field1 field2 .. fieldn)`.
* a struct with arguments as `(field1 field2 .. fieldn)`
* a struct with no arguments as the empty string.
* a `Vec` as `[elem1 elem2 .. elemn]`
* a tuple as `{elem1 elem2 .. elemn}`
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

The convenience cannot be used if you have a struct whose only argument is a
non-unit enum (resp. a struct with multiple arguments). In this case, you
should use two sets of parentheses, e.g.
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
    rti: &ReflectedTypeInfo,
    ctx: &mut C,
) -> Result<D, String>
where
    C: TestDeserializeContext,
    D: DeserializeOwned,
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
Thus, you need to derive the trait `MzReflect` for
each enum or struct that you will need to construct your object.

Default values are supported as long as the default fields are last. Put
`#[serde(default)]` over any fields you want to be default and make sure those
fields have default values.

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

### 3. Feeding in type information

Enum/struct information must be passed into the converter via
the struct `ReflectedTypeInfo`. For your convenience, there is a macro to
generate a function will produce the right `ReflectedTypeInfo` for the object
you are trying to create.
```
get_reflect_info_func!(func_name, [EnumType1, .., EnumTypeN], [StructType1, ..,StructTypeN])
```
Calling `func_name()` will produce the desired `ReflectedTypeInfo`.

For instructions on how to manually populate `ReflectedTypeInfo`, see the struct
documentation.

### 4. Feeding in syntax extensions

If you don't need to extend or override the syntax, give a
`&mut GenericTestDeserializationContext::default()`.

Otherwise, see [#extending-the-syntax].

## Extending the syntax

Create an object that implements the trait `TestDeserializationContext`.

The trait has a method `override_syntax`.
* Its first argument is `&mut self` this way the `TestDeserializationContext`
  can store state across multiple objects being created.
* The return value should always be `Ok(None)` except in the specific cases when
  you are overriding or extending the syntax, in which case the return value
  will be either `Ok(Some(JSON_string))` or `Err(err_string)`.
* The second and third arguments of the method is the first element in the
  stream and a pointer to the rest of the stream respectively. The contract of
  `override_syntax` is that you promise not to iterate through the stream more
  than is necessary to construct your object. If you return `Ok(None)`, you
  should have left the stream untouched.
* The fourth argument is the type of the object whose specification is currently
  being translated. Note that the name of the type is only guaranteed to be
  accurate if it is a [supported type](supported-types). Thus, if an enum
  variant (resp. struct) you are trying to create has a field that is not a
  supported type, you should use `override_syntax` to specify how to create the
  enum variant (resp. struct).

Refer to the [proc_macro2] docs for information how to parse the stream.

[proc_macro2]: https://docs.rs/proc-macro2/1.0.27/proc_macro2/enum.TokenTree.html

If an object being created is registered as an enum or struct in
`ReflectedTypeInfo`, what is passed to `override_syntax` changes:
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
to the extended syntax. See [#roundtrip] for more details.

### Supported types

* Enums
* Structs
* Primitive types
* Strings
* Tuples
* `Box<>`
* `Option<>`
* `Vec<>`
* Combinations of the above.

## Roundtrip

To convert a Rust object back into the readable syntax:
1) Serialize the Rust object to a [serde_json::Value] object by calling
   `let json = serde_json::to_value(obj)?;`.
2) Feed the json into the method `from_json`.
