[![Gitter](https://badges.gitter.im/rust-mysql/community.svg)](https://gitter.im/rust-mysql/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

[![Crates.io](https://img.shields.io/crates/v/mysql_common.svg)](https://crates.io/crates/mysql_common)
[![Docs.rs](https://docs.rs/mysql_common/badge.svg)](https://docs.rs/mysql_common)
[![Build Status](https://travis-ci.org/blackbeam/rust_mysql_common.svg?branch=master)](https://travis-ci.org/blackbeam/rust_mysql_common)

# mysql_common

This crate is an implementation of basic MySql protocol primitives.

This crate:
* defines basic MySql constants;
* implements necessary functionality for MySql `cached_sha2_password`,
  `mysql_native_password` and legacy authentication plugins;
* implements helper traits for MySql protocol IO;
* implements support of named parameters for prepared statements;
* implements parsers for a subset of MySql protocol packets (including binlog packets);
* defines rust representation of MySql protocol values and rows;
* implements conversion between MySql values and rust types, between MySql rows and tuples
  of rust types.
* implements [FromRow and FromValue derive macros][2]

### Supported rust types

Crate offers conversion from/to MySql values for following types (please see MySql documentation
on supported ranges for numeric types). Following table refers to MySql protocol types
(see `Value` struct) and not to MySql column types. Please see [MySql documentation][1] for
column and protocol type correspondence:

| Type                                 | Notes                                                     |
| ------------------------------------ | -------------------------------------------------------   |
| `{i,u}8..{i,u}128`, `{i,u}size`      | MySql int/uint will be converted, bytes will be parsed.<br>⚠️ Note that range of `{i,u}128` is greater than supported by MySql integer types but it'll be serialized anyway (as decimal bytes string). |
| `f32`                                | MySql float will be converted to `f32`, bytes will be parsed as `f32`.<br>⚠️ MySql double won't be converted to `f32` to avoid precision loss (see #17) |
| `f64`                                | MySql float and double will be converted to `f64`, bytes will be parsed as `f64`. |
| `bool`                               | MySql int {`0`, `1`} or bytes {`"0x30"`, `"0x31"`}        |
| `Vec<u8>`                            | MySql bytes                                               |
| `String`                             | MySql bytes parsed as utf8                                |
| `Duration` (`std` and `time`)        | MySql time or bytes parsed as MySql time string           |
| [`time::PrimitiveDateTime`] (v0.2.x) | MySql date time or bytes parsed as MySql date time string (⚠️ lossy! microseconds are ignored)           |
| [`time::Date`] (v0.2.x)              | MySql date or bytes parsed as MySql date string (⚠️ lossy! microseconds are ignored)           |
| [`time::Time`] (v0.2.x)              | MySql time or bytes parsed as MySql time string (⚠️ lossy! microseconds are ignored)           |
| [`time::Duration`] (v0.2.x)          | MySql time or bytes parsed as MySql time string           |
| [`time::PrimitiveDateTime`] (v0.3.x) | MySql date time or bytes parsed as MySql date time string (⚠️ lossy! microseconds are ignored)           |
| [`time::Date`] (v0.3.x)              | MySql date or bytes parsed as MySql date string (⚠️ lossy! microseconds are ignored)           |
| [`time::Time`] (v0.3.x)              | MySql time or bytes parsed as MySql time string (⚠️ lossy! microseconds are ignored)           |
| [`time::Duration`] (v0.3.x)          | MySql time or bytes parsed as MySql time string           |
| [`chrono::NaiveTime`]                | MySql date or bytes parsed as MySql date string           |
| [`chrono::NaiveDate`]                | MySql date or bytes parsed as MySql date string           |
| [`chrono::NaiveDateTime`]            | MySql date or bytes parsed as MySql date string           |
| [`uuid::Uuid`]                       | MySql bytes parsed using `Uuid::from_slice`               |
| [`serde_json::Value`]                | MySql bytes parsed using `serde_json::from_str`           |
| `mysql_common::Deserialized<T : DeserializeOwned>` | MySql bytes parsed using `serde_json::from_str` |
| `Option<T: FromValue>`               | Must be used for nullable columns to avoid errors         |
| [`decimal::Decimal`]                 | MySql int, uint or bytes parsed using `Decimal::from_str`.<br>⚠️ Note that this type doesn't support full range of MySql `DECIMAL` type. |
| [`bigdecimal::BigDecimal`] (v0.2.x)  | MySql int, uint, floats or bytes parsed using `BigDecimal::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql `DECIMAL` type but it'll be serialized anyway. |
| [`bigdecimal::BigDecimal`] (v0.3.x)  | MySql int, uint, floats or bytes parsed using `BigDecimal::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql `DECIMAL` type but it'll be serialized anyway. |
| [`bigdecimal::BigDecimal`] (v0.4.x)  | MySql int, uint, floats or bytes parsed using `BigDecimal::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql `DECIMAL` type but it'll be serialized anyway. |
| `num_bigint::{BigInt, BigUint}`      | MySql int, uint or bytes parsed using `_::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql integer types but it'll be serialized anyway (as decimal bytes string). |

Also crate provides from-row convertion for the following list of types (see `FromRow` trait):

| Type                                            | Notes                                             |
| ----------------------------------------------- | ------------------------------------------------- |
| `Row`                                           | Trivial conversion for `Row` itself.              |
| `T: FromValue`                                  | For rows with a single column.                    |
| `(T1: FromValue [, ..., T12: FromValue])`       | Row to a tuple of arity 1-12.                     |
| [`frunk::hlist::HList`] types                   | Usefull to overcome tuple arity limitation        |

### Crate features

| Feature        | Description                                          | Default |
| -------------- | ---------------------------------------------------- | ------- |
| `bigdecimal02` | Enables `bigdecimal` v0.2.x types support            | 🔴      |
| `bigdecimal03` | Enables `bigdecimal` v0.3.x types support            | 🔴      |
| `bigdecimal`   | Enables `bigdecimal` v0.4.x types support            | 🟢      |
| `chrono`       | Enables `chrono` types support                       | 🔴      |
| `rust_decimal` | Enables `rust_decimal` types support                 | 🟢      |
| `time02`       | Enables `time` v0.2.x types support                  | 🔴      |
| `time`         | Enables `time` v0.3.x types support                  | 🟢      |
| `frunk`        | Enables `FromRow` for `frunk::Hlist!` types          | 🟢      |
| `derive`       | Enables [`FromValue` and `FromRow` derive macros][2] | 🟢      |
| `binlog`       | Binlog-related functionality                         | 🟢      |

## Derive Macros

### `FromValue` Derive

Supported derivations:

*   for enum – you should carefully read the [corresponding section of MySql documentation][4].
*   for newtypes (see [New Type Idiom][3]) – given that the wrapped type itself satisfies
    `FromValue`.

#### Enums

##### Container attributes:

*  `#[mysql(crate_name = "some_name")]` – overrides an attempt to guess a crate that provides
   required traits
*  `#[mysql(rename_all = ...)]` – rename all the variants according to the given case
   convention. The possible values are "lowercase", "UPPERCASE", "PascalCase", "camelCase",
   "snake_case", "SCREAMING_SNAKE_CASE", "kebab-case", "SCREAMING-KEBAB-CASE"
*  `#[mysql(is_integer)]` – tells derive macro that the value is an integer rather than MySql
   ENUM. Macro won't warn if variants are sparse or greater than u16 and will not try to parse
   textual representation.
*  `#[mysql(is_string)]` – tells derive macro that the value is a string rather than MySql
   ENUM. Macro won't warn if variants are sparse or greater than u16 and will not try to parse
   integer representation.

##### Example

Given `ENUM('x-small', 'small', 'medium', 'large', 'x-large')` on MySql side:

```rust

fn main() {

/// Note: the `crate_name` attribute should not be necessary.
#[derive(FromValue)]
#[mysql(rename_all = "kebab-case", crate_name = "mysql_common")]
#[repr(u8)]
enum Size {
    XSmall = 1,
    Small,
    Medium,
    Large,
    XLarge,
}

fn assert_from_row_works(x: Row) -> Size {
    from_row(x)
}

}
```

#### Newtypes

It is expected, that wrapper value satisfies `FromValue` or `deserialize_with` is given.
Also note, that to support `FromRow` the wrapped value must satisfy `Into<Value>` or
`serialize_with` must be given.

##### Container attributes:

*  `#[mysql(crate_name = "some_name")]` – overrides an attempt to guess a crate to import types from
*  `#[mysql(bound = "Foo: Bar, Baz: Quux")]` – use the following additional bounds
*  `#[mysql(deserialize_with = "some::path")]` – use the following function to deserialize
   the wrapped value. Expected signature is `fn (Value) -> Result<Wrapped, FromValueError>`.
*  `#[mysql(serialize_with = "some::path")]` – use the following function to serialize
   the wrapped value. Expected signature is `fn (Wrapped) -> Value`.

##### Example

```rust

/// Trivial example
#[derive(FromValue)]
struct Inch(i32);

/// Example of a {serialize|deserialize}_with.
#[derive(FromValue)]
#[mysql(deserialize_with = "neg_de", serialize_with = "neg_ser")]
struct Neg(i64);

/// Wrapped generic. Bounds are inferred.
#[derive(FromValue)]
struct Foo<T>(Option<T>);

/// Example of additional bounds.
#[derive(FromValue)]
#[mysql(bound = "'b: 'a, T: 'a, U: From<String>, V: From<u64>")]
struct Bar<'a, 'b, const N: usize, T, U, V>(ComplexTypeToWrap<'a, 'b, N, T, U, V>);

fn assert_from_row_works<'a, 'b, const N: usize, T, U, V>(x: Row) -> (Inch, Neg, Foo<u8>, Bar<'a, 'b, N, T, U, V>)
where 'b: 'a, T: 'a, U: From<String>, V: From<u64>,
{
    from_row(x)
}


// test boilerplate..


/// Dummy complex type with additional bounds on FromValue impl.
struct ComplexTypeToWrap<'a, 'b, const N: usize, T, U, V>([(&'a T, &'b U, V); N]);

struct FakeIr;

impl TryFrom<Value> for FakeIr {
    // ...
}

impl<'a, 'b: 'a, const N: usize, T: 'a, U: From<String>, V: From<u64>> From<FakeIr> for ComplexTypeToWrap<'a, 'b, N, T, U, V> {
    // ...
}

impl From<FakeIr> for Value {
    // ...
}

impl<'a, 'b: 'a, const N: usize, T: 'a, U: From<String>, V: From<u64>> FromValue for ComplexTypeToWrap<'a, 'b, N, T, U, V> {
    type Intermediate = FakeIr;
}

fn neg_de(v: Value) -> Result<i64, FromValueError> {
    match v {
        Value::Int(x) => Ok(-x),
        Value::UInt(x) => Ok(-(x as i64)),
        x => Err(FromValueError(x)),
    }
}

fn neg_ser(x: i64) -> Value {
    Value::Int(-x)
}

```

### `FromRow` Derive

Also defines some constants on the struct:

*  `const TABLE_NAME: &str` – if `table_name` is given
*  `const {}_FIELD: &str` – for each struct field (`{}` is a SCREAMING_SNAKE_CASE representation
   of a struct field name (not a column name))

Supported derivations:

* for a struct with named fields – field name will be used as a column name to search for a value

#### Container attributes:

*  `#[mysql(crate_name = "some_name")]` – overrides an attempt to guess a crate that provides
   required traits
*  `#[mysql(rename_all = ...)]` – rename all column names according to the given case
   convention. The possible values are "lowercase", "UPPERCASE", "PascalCase", "camelCase",
   "snake_case", "SCREAMING_SNAKE_CASE", "kebab-case", "SCREAMING-KEBAB-CASE"
*  `#[mysql(table_name = "some_name")]` – defines `pub const TABLE_NAME: &str` on the struct

#### Field attributes:

*  `#[mysql(rename = "some_name")]` – overrides column name of a field
*  `#[mysql(json)]` - column will be interpreted as a JSON string containing
   a value of a field type

#### Example

```rust

/// Note: the `crate_name` attribute should not be necessary.
#[derive(Debug, PartialEq, Eq, FromRow)]
#[mysql(table_name = "Foos", crate_name = "mysql_common")]
struct Foo {
    id: u64,
    #[mysql(json, rename = "def")]
    definition: Bar,
    child: Option<u64>,
}

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
enum Bar {
    Left,
    Right,
}

/// Returns the following row:
///
/// ```
/// +----+-----------+-------+
/// | id | def       | child |
/// +----+-----------+-------+
/// | 42 | '"Right"' | NULL  |
/// +----+-----------+-------+
/// ```
fn get_row() -> Row {
    // ...
}

assert_eq!(Foo::TABLE_NAME, "Foos");
assert_eq!(Foo::ID_FIELD, "id");
assert_eq!(Foo::DEFINITION_FIELD, "def");
assert_eq!(Foo::CHILD_FIELD, "child");

let foo = from_row::<Foo>(get_row());
assert_eq!(foo, Foo { id: 42, definition: Bar::Right, child: None });
```

[1]: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
[2]: #derive-macros
[3]: https://doc.rust-lang.org/rust-by-example/generics/new_types.html
[4]: https://dev.mysql.com/doc/refman/8.0/en/enum.html

## License

Licensed under either of
 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
