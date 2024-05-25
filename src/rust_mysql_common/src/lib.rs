// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This crate is an implementation of basic MySql protocol primitives.
//!
//! This crate:
//! * defines basic MySql constants;
//! * implements necessary functionality for MySql `cached_sha2_password`,
//!   `mysql_native_password` and legacy authentication plugins;
//! * implements helper traits for MySql protocol IO;
//! * implements support of named parameters for prepared statements;
//! * implements parsers for a subset of MySql protocol packets (including binlog packets);
//! * defines rust representation of MySql protocol values and rows;
//! * implements conversion between MySql values and rust types, between MySql rows and tuples
//!   of rust types.
//! * implements [FromRow and FromValue derive macros][2]
//!
//! ## Supported rust types
//!
//! Crate offers conversion from/to MySql values for following types (please see MySql documentation
//! on supported ranges for numeric types). Following table refers to MySql protocol types
//! (see `Value` struct) and not to MySql column types. Please see [MySql documentation][1] for
//! column and protocol type correspondence:
//!
//! | Type                                 | Notes                                                     |
//! | ------------------------------------ | -------------------------------------------------------   |
//! | `{i,u}8..{i,u}128`, `{i,u}size`      | MySql int/uint will be converted, bytes will be parsed.<br>⚠️ Note that range of `{i,u}128` is greater than supported by MySql integer types but it'll be serialized anyway (as decimal bytes string). |
//! | `f32`                                | MySql float will be converted to `f32`, bytes will be parsed as `f32`.<br>⚠️ MySql double won't be converted to `f32` to avoid precision loss (see #17) |
//! | `f64`                                | MySql float and double will be converted to `f64`, bytes will be parsed as `f64`. |
//! | `bool`                               | MySql int {`0`, `1`} or bytes {`"0x30"`, `"0x31"`}        |
//! | `Vec<u8>`                            | MySql bytes                                               |
//! | `String`                             | MySql bytes parsed as utf8                                |
//! | `Duration` (`std` and `time`)        | MySql time or bytes parsed as MySql time string           |
//! | [`time::PrimitiveDateTime`] (v0.2.x) | MySql date time or bytes parsed as MySql date time string (⚠️ lossy! microseconds are ignored)           |
//! | [`time::Date`] (v0.2.x)              | MySql date or bytes parsed as MySql date string (⚠️ lossy! microseconds are ignored)           |
//! | [`time::Time`] (v0.2.x)              | MySql time or bytes parsed as MySql time string (⚠️ lossy! microseconds are ignored)           |
//! | [`time::Duration`] (v0.2.x)          | MySql time or bytes parsed as MySql time string           |
//! | [`time::PrimitiveDateTime`] (v0.3.x) | MySql date time or bytes parsed as MySql date time string (⚠️ lossy! microseconds are ignored)           |
//! | [`time::Date`] (v0.3.x)              | MySql date or bytes parsed as MySql date string (⚠️ lossy! microseconds are ignored)           |
//! | [`time::Time`] (v0.3.x)              | MySql time or bytes parsed as MySql time string (⚠️ lossy! microseconds are ignored)           |
//! | [`time::Duration`] (v0.3.x)          | MySql time or bytes parsed as MySql time string           |
//! | [`chrono::NaiveTime`]                | MySql date or bytes parsed as MySql date string           |
//! | [`chrono::NaiveDate`]                | MySql date or bytes parsed as MySql date string           |
//! | [`chrono::NaiveDateTime`]            | MySql date or bytes parsed as MySql date string           |
//! | [`uuid::Uuid`]                       | MySql bytes parsed using `Uuid::from_slice`               |
//! | [`serde_json::Value`]                | MySql bytes parsed using `serde_json::from_str`           |
//! | `mysql_common::Deserialized<T : DeserializeOwned>` | MySql bytes parsed using `serde_json::from_str` |
//! | `Option<T: FromValue>`               | Must be used for nullable columns to avoid errors         |
//! | [`decimal::Decimal`]                 | MySql int, uint or bytes parsed using `Decimal::from_str`.<br>⚠️ Note that this type doesn't support full range of MySql `DECIMAL` type. |
//! | [`bigdecimal::BigDecimal`] (v0.2.x)  | MySql int, uint, floats or bytes parsed using `BigDecimal::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql `DECIMAL` type but it'll be serialized anyway. |
//! | [`bigdecimal::BigDecimal`] (v0.3.x)  | MySql int, uint, floats or bytes parsed using `BigDecimal::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql `DECIMAL` type but it'll be serialized anyway. |
//! | [`bigdecimal::BigDecimal`] (v0.4.x)  | MySql int, uint, floats or bytes parsed using `BigDecimal::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql `DECIMAL` type but it'll be serialized anyway. |
//! | `num_bigint::{BigInt, BigUint}`      | MySql int, uint or bytes parsed using `_::parse_bytes`.<br>⚠️ Note that range of this type is greater than supported by MySql integer types but it'll be serialized anyway (as decimal bytes string). |
//!
//! Also crate provides from-row convertion for the following list of types (see `FromRow` trait):
//!
//! | Type                                            | Notes                                             |
//! | ----------------------------------------------- | ------------------------------------------------- |
//! | `Row`                                           | Trivial conversion for `Row` itself.              |
//! | `T: FromValue`                                  | For rows with a single column.                    |
//! | `(T1: FromValue [, ..., T12: FromValue])`       | Row to a tuple of arity 1-12.                     |
//! | [`frunk::hlist::HList`] types                   | Usefull to overcome tuple arity limitation        |
//!
//! ## Crate features
//!
//! | Feature        | Description                                          | Default |
//! | -------------- | ---------------------------------------------------- | ------- |
//! | `bigdecimal02` | Enables `bigdecimal` v0.2.x types support            | 🔴      |
//! | `bigdecimal03` | Enables `bigdecimal` v0.3.x types support            | 🔴      |
//! | `bigdecimal`   | Enables `bigdecimal` v0.4.x types support            | 🟢      |
//! | `chrono`       | Enables `chrono` types support                       | 🔴      |
//! | `rust_decimal` | Enables `rust_decimal` types support                 | 🟢      |
//! | `time02`       | Enables `time` v0.2.x types support                  | 🔴      |
//! | `time`         | Enables `time` v0.3.x types support                  | 🟢      |
//! | `frunk`        | Enables `FromRow` for `frunk::Hlist!` types          | 🟢      |
//! | `derive`       | Enables [`FromValue` and `FromRow` derive macros][2] | 🟢      |
//! | `binlog`       | Binlog-related functionality                         | 🟢      |
//!
//! # Derive Macros
//!
//! ## `FromValue` Derive
//!
//! Supported derivations:
//!
//! *   for enum – you should carefully read the [corresponding section of MySql documentation][4].
//! *   for newtypes (see [New Type Idiom][3]) – given that the wrapped type itself satisfies
//!     `FromValue`.
//!
//! ### Enums
//!
//! #### Container attributes:
//!
//! *  `#[mysql(crate_name = "some_name")]` – overrides an attempt to guess a crate that provides
//!    required traits
//! *  `#[mysql(rename_all = ...)]` – rename all the variants according to the given case
//!    convention. The possible values are "lowercase", "UPPERCASE", "PascalCase", "camelCase",
//!    "snake_case", "SCREAMING_SNAKE_CASE", "kebab-case", "SCREAMING-KEBAB-CASE"
//! *  `#[mysql(is_integer)]` – tells derive macro that the value is an integer rather than MySql
//!    ENUM. Macro won't warn if variants are sparse or greater than u16 and will not try to parse
//!    textual representation.
//! *  `#[mysql(is_string)]` – tells derive macro that the value is a string rather than MySql
//!    ENUM. Macro won't warn if variants are sparse or greater than u16 and will not try to parse
//!    integer representation.
//!
//! #### Example
//!
//! Given `ENUM('x-small', 'small', 'medium', 'large', 'x-large')` on MySql side:
//!
//! ```no_run
//! # use mysql_common_derive::FromValue;
//! # use mysql_common::{row::Row, row::convert::from_row};
//!
//! fn main() {
//!
//! /// Note: the `crate_name` attribute should not be necessary.
//! #[derive(FromValue)]
//! #[mysql(rename_all = "kebab-case", crate_name = "mysql_common")]
//! #[repr(u8)]
//! enum Size {
//!     XSmall = 1,
//!     Small,
//!     Medium,
//!     Large,
//!     XLarge,
//! }
//!
//! fn assert_from_row_works(x: Row) -> Size {
//!     from_row(x)
//! }
//!
//! }
//! ```
//!
//! ### Newtypes
//!
//! It is expected, that wrapper value satisfies `FromValue` or `deserialize_with` is given.
//! Also note, that to support `FromRow` the wrapped value must satisfy `Into<Value>` or
//! `serialize_with` must be given.
//!
//! #### Container attributes:
//!
//! *  `#[mysql(crate_name = "some_name")]` – overrides an attempt to guess a crate to import types from
//! *  `#[mysql(bound = "Foo: Bar, Baz: Quux")]` – use the following additional bounds
//! *  `#[mysql(deserialize_with = "some::path")]` – use the following function to deserialize
//!    the wrapped value. Expected signature is `fn (Value) -> Result<Wrapped, FromValueError>`.
//! *  `#[mysql(serialize_with = "some::path")]` – use the following function to serialize
//!    the wrapped value. Expected signature is `fn (Wrapped) -> Value`.
//!
//! #### Example
//!
//! ```no_run
//! # use mysql_common::{row::Row, row::convert::from_row, prelude::FromValue, value::Value, value::convert::{from_value, FromValueError}};
//! # use std::convert::TryFrom;
//!
//! /// Trivial example
//! #[derive(FromValue)]
//! # #[mysql(crate_name = "mysql_common")]
//! struct Inch(i32);
//!
//! /// Example of a {serialize|deserialize}_with.
//! #[derive(FromValue)]
//! # #[mysql(crate_name = "mysql_common")]
//! #[mysql(deserialize_with = "neg_de", serialize_with = "neg_ser")]
//! struct Neg(i64);
//!
//! /// Wrapped generic. Bounds are inferred.
//! #[derive(FromValue)]
//! # #[mysql(crate_name = "mysql_common")]
//! struct Foo<T>(Option<T>);
//!
//! /// Example of additional bounds.
//! #[derive(FromValue)]
//! # #[mysql(crate_name = "mysql_common")]
//! #[mysql(bound = "'b: 'a, T: 'a, U: From<String>, V: From<u64>")]
//! struct Bar<'a, 'b, const N: usize, T, U, V>(ComplexTypeToWrap<'a, 'b, N, T, U, V>);
//!
//! fn assert_from_row_works<'a, 'b, const N: usize, T, U, V>(x: Row) -> (Inch, Neg, Foo<u8>, Bar<'a, 'b, N, T, U, V>)
//! where 'b: 'a, T: 'a, U: From<String>, V: From<u64>,
//! {
//!     from_row(x)
//! }
//!
//!
//! // test boilerplate..
//!
//!
//! /// Dummy complex type with additional bounds on FromValue impl.
//! struct ComplexTypeToWrap<'a, 'b, const N: usize, T, U, V>([(&'a T, &'b U, V); N]);
//!
//! struct FakeIr;
//!
//! impl TryFrom<Value> for FakeIr {
//!     // ...
//! #    type Error = FromValueError;
//! #    fn try_from(v: Value) -> Result<Self, Self::Error> {
//! #        unimplemented!();
//! #    }
//! }
//!
//! impl<'a, 'b: 'a, const N: usize, T: 'a, U: From<String>, V: From<u64>> From<FakeIr> for ComplexTypeToWrap<'a, 'b, N, T, U, V> {
//!     // ...
//! #    fn from(x: FakeIr) -> Self {
//! #        unimplemented!();
//! #    }
//! }
//!
//! impl From<FakeIr> for Value {
//!     // ...
//! #    fn from(x: FakeIr) -> Self {
//! #        unimplemented!();
//! #    }
//! }
//!
//! impl<'a, 'b: 'a, const N: usize, T: 'a, U: From<String>, V: From<u64>> FromValue for ComplexTypeToWrap<'a, 'b, N, T, U, V> {
//!     type Intermediate = FakeIr;
//! }
//!
//! fn neg_de(v: Value) -> Result<i64, FromValueError> {
//!     match v {
//!         Value::Int(x) => Ok(-x),
//!         Value::UInt(x) => Ok(-(x as i64)),
//!         x => Err(FromValueError(x)),
//!     }
//! }
//!
//! fn neg_ser(x: i64) -> Value {
//!     Value::Int(-x)
//! }
//!
//! # fn main() {}
//! ```
//!
//! ## `FromRow` Derive
//!
//! Also defines some constants on the struct:
//!
//! *  `const TABLE_NAME: &str` – if `table_name` is given
//! *  `const {}_FIELD: &str` – for each struct field (`{}` is a SCREAMING_SNAKE_CASE representation
//!    of a struct field name (not a column name))
//!
//! Supported derivations:
//!
//! * for a struct with named fields – field name will be used as a column name to search for a value
//!
//! ### Container attributes:
//!
//! *  `#[mysql(crate_name = "some_name")]` – overrides an attempt to guess a crate that provides
//!    required traits
//! *  `#[mysql(rename_all = ...)]` – rename all column names according to the given case
//!    convention. The possible values are "lowercase", "UPPERCASE", "PascalCase", "camelCase",
//!    "snake_case", "SCREAMING_SNAKE_CASE", "kebab-case", "SCREAMING-KEBAB-CASE"
//! *  `#[mysql(table_name = "some_name")]` – defines `pub const TABLE_NAME: &str` on the struct
//!
//! ### Field attributes:
//!
//! *  `#[mysql(rename = "some_name")]` – overrides column name of a field
//! *  `#[mysql(json)]` - column will be interpreted as a JSON string containing
//!    a value of a field type
//! *  `#[mysql(with = path::to::convert_fn)]` – `convert_fn` will be used to deserialize
//!    a field value (expects a function with a signature that mimics
//!    `TryFrom<Value, Error=FromValueError>``)
//!
//! ### Example
//!
//! ```
//! # use mysql_common_derive::FromRow;
//! # use mysql_common::{
//! #     constants::ColumnType,
//! #     packets::Column,
//! #     row::{Row, new_row},
//! #     row::convert::from_row,
//! #     value::Value,
//! # };
//!
//! /// Note: the `crate_name` attribute should not be necessary.
//! #[derive(Debug, PartialEq, Eq, FromRow)]
//! #[mysql(table_name = "Foos", crate_name = "mysql_common")]
//! struct Foo {
//!     id: u64,
//!     #[mysql(json, rename = "def")]
//!     definition: Bar,
//!     child: Option<u64>,
//! }
//!
//! #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
//! enum Bar {
//!     Left,
//!     Right,
//! }
//!
//! /// Returns the following row:
//! ///
//! /// ```
//! /// +----+-----------+-------+
//! /// | id | def       | child |
//! /// +----+-----------+-------+
//! /// | 42 | '"Right"' | NULL  |
//! /// +----+-----------+-------+
//! /// ```
//! fn get_row() -> Row {
//!     // ...
//! #   let values = vec![Value::Int(42), Value::Bytes(b"\"Right\"".as_slice().into()), Value::NULL];
//! #   let columns = vec![
//! #       Column::new(ColumnType::MYSQL_TYPE_LONG).with_name(b"id"),
//! #       Column::new(ColumnType::MYSQL_TYPE_BLOB).with_name(b"def"),
//! #       Column::new(ColumnType::MYSQL_TYPE_NULL).with_name(b"child"),
//! #   ];
//! #   new_row(values, columns.into_boxed_slice().into())
//! }
//!
//! # fn main() {
//! assert_eq!(Foo::TABLE_NAME, "Foos");
//! assert_eq!(Foo::ID_FIELD, "id");
//! assert_eq!(Foo::DEFINITION_FIELD, "def");
//! assert_eq!(Foo::CHILD_FIELD, "child");
//!
//! let foo = from_row::<Foo>(get_row());
//! assert_eq!(foo, Foo { id: 42, definition: Bar::Right, child: None });
//! # }
//! ```
//!
//! [1]: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html
//! [2]: #derive-macros
//! [3]: https://doc.rust-lang.org/rust-by-example/generics/new_types.html
//! [4]: https://dev.mysql.com/doc/refman/8.0/en/enum.html
#![cfg_attr(feature = "nightly", feature(test))]
#![cfg_attr(docsrs, feature(doc_cfg))]

// The `test` feature is required to compile tests.
// It'll bind test binaries to an official C++ impl of MySql decimals (see build.rs)
// The idea is to test our rust impl agaist C++ impl.
#[cfg(all(not(feature = "test"), test))]
compile_error!("Please invoke `cargo test` with `--features test` flags");

#[cfg(feature = "nightly")]
extern crate test;

#[macro_use]
pub mod bitflags_ext;

#[cfg(feature = "bigdecimal02")]
pub use bigdecimal02;

#[cfg(feature = "bigdecimal03")]
pub use bigdecimal03;

#[cfg(feature = "bigdecimal")]
pub use bigdecimal;

#[cfg(feature = "chrono")]
pub use chrono;

#[cfg(feature = "frunk")]
pub use frunk;

#[cfg(feature = "rust_decimal")]
pub use rust_decimal;

#[cfg(feature = "time02")]
pub use time02;

#[cfg(feature = "time")]
pub use time;

pub use uuid;

#[cfg(feature = "derive")]
#[allow(unused_imports)]
#[macro_use]
extern crate mysql_common_derive;

pub use num_bigint;
pub use serde;
pub use serde_json;

pub use value::convert::FromValueError;
pub use value::Value;

pub use row::convert::FromRowError;
pub use row::Row;

pub use value::json::{Deserialized, Serialized};

pub mod prelude {
    #[cfg(feature = "derive")]
    #[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
    #[doc(inline)]
    pub use mysql_common_derive::FromValue;

    #[cfg(feature = "derive")]
    #[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
    #[doc(inline)]
    pub use mysql_common_derive::FromRow;

    pub use crate::row::{convert::FromRow, ColumnIndex};
    pub use crate::value::convert::{FromValue, ToValue};
}

/// This macro is a convenient way to pass named parameters to a statement.
///
/// ```ignore
/// let foo = 42;
/// conn.prep_exec("SELECT :foo, :foo2x", params! {
///     foo,
///     "foo2x" => foo * 2,
/// });
/// ```
#[macro_export]
macro_rules! params {
    () => {};
    (@to_pair $map:expr, $name:expr => $value:expr) => (
        let entry = $map.entry(std::vec::Vec::<u8>::from($name));
        if let std::collections::hash_map::Entry::Occupied(_) = entry {
            panic!("Redefinition of named parameter `{}'", std::string::String::from_utf8_lossy(entry.key()));
        } else {
            entry.or_insert($crate::value::Value::from($value));
        }
    );
    (@to_pair $map:expr, $name:ident) => (
        let entry = $map.entry(stringify!($name).as_bytes().to_vec());
        if let std::collections::hash_map::Entry::Occupied(_) = entry {
            panic!("Redefinition of named parameter `{}'", std::string::String::from_utf8_lossy(entry.key()));
        } else {
            entry.or_insert($crate::value::Value::from($name));
        }
    );
    (@expand $map:expr;) => {};
    (@expand $map:expr; $name:expr => $value:expr, $($tail:tt)*) => {
        params!(@to_pair $map, $name => $value);
        params!(@expand $map; $($tail)*);
    };
    (@expand $map:expr; $name:expr => $value:expr $(, $tail:tt)*) => {
        params!(@to_pair $map, $name => $value);
        params!(@expand $map; $($tail)*);
    };
    (@expand $map:expr; $name:ident, $($tail:tt)*) => {
        params!(@to_pair $map, $name);
        params!(@expand $map; $($tail)*);
    };
    (@expand $map:expr; $name:ident $(, $tail:tt)*) => {
        params!(@to_pair $map, $name);
        params!(@expand $map; $($tail)*);
    };
    ($i:ident, $($tail:tt)*) => {
        {
            let mut map: std::collections::HashMap<std::vec::Vec<u8>, $crate::value::Value, _> = std::default::Default::default();
            params!(@expand (&mut map); $i, $($tail)*);
            $crate::params::Params::Named(map)
        }
    };
    ($i:expr => $($tail:tt)*) => {
        {
            let mut map: std::collections::HashMap<std::vec::Vec<u8>, $crate::value::Value, _> = std::default::Default::default();
            params!(@expand (&mut map); $i => $($tail)*);
            $crate::params::Params::Named(map)
        }
    };
    ($i:ident) => {
        {
            let mut map: std::collections::HashMap<std::vec::Vec<u8>, $crate::value::Value, _> = std::default::Default::default();
            params!(@expand (&mut map); $i);
            $crate::params::Params::Named(map)
        }
    }
}

pub mod collations;
pub mod constants;
pub mod crypto;
pub mod io;
pub mod misc;
pub mod named_params;
#[macro_use]
pub mod packets;
pub mod params;
pub mod proto;
pub mod row;
pub mod scramble;
pub mod value;

#[cfg(feature = "binlog")]
#[cfg_attr(docsrs, doc(cfg(feature = "binlog")))]
pub mod binlog;

#[cfg(test)]
#[test]
fn params_macro_test() {
    use crate::{params::Params, value::Value};

    let foo = 42;
    let bar = "bar";

    assert_eq!(
        Params::from(vec![(String::from("foo"), Value::Int(42))]),
        params! { foo }
    );
    assert_eq!(
        Params::from(vec![(String::from("foo"), Value::Int(42))]),
        params! { foo, }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { foo, bar }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { foo, bar, }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { "foo" => foo, "bar" => bar }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { "foo" => foo, "bar" => bar, }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { foo, "bar" => bar }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { "foo" => foo, bar }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { foo, "bar" => bar, }
    );
    assert_eq!(
        Params::from(vec![
            (String::from("foo"), Value::Int(42)),
            (String::from("bar"), Value::Bytes((&b"bar"[..]).into())),
        ]),
        params! { "foo" => foo, bar, }
    );
}

#[test]
#[should_panic(expected = "Redefinition of named parameter `a'")]
fn params_macro_should_panic_on_named_param_redefinition() {
    params! {"a" => 1, "b" => 2, "a" => 3};
}

#[test]
fn issue_88() {
    use crate::{prelude::FromValue, Value};
    #[derive(FromValue, Debug, Eq, PartialEq)]
    #[mysql(is_integer)]
    #[repr(u8)]
    enum SomeType {
        A,
        B = 42,
        C,
    }

    let value = Value::Int(42);
    assert_eq!(SomeType::B, SomeType::from_value(value));

    let value = Value::Int(0);
    assert_eq!(SomeType::A, SomeType::from_value(value));
}

#[test]
fn from_value_is_string() {
    use crate::{prelude::FromValue, Value};
    #[derive(FromValue, Debug, Eq, PartialEq)]
    #[mysql(is_string, rename_all = "snake_case")]
    enum SomeTypeIsString {
        FirstVariant = 0,
        SecondVariant = 2,
        ThirdVariant = 3,
    }

    let value = Value::Bytes(b"first_variant".to_vec());
    assert_eq!(
        SomeTypeIsString::FirstVariant,
        SomeTypeIsString::from_value(value)
    );

    let value = Value::Bytes(b"third_variant".to_vec());
    assert_eq!(
        SomeTypeIsString::ThirdVariant,
        SomeTypeIsString::from_value(value)
    );

    assert_eq!(
        Value::from(SomeTypeIsString::FirstVariant),
        Value::Bytes(b"first_variant".to_vec())
    );
    assert_eq!(
        Value::from(SomeTypeIsString::SecondVariant),
        Value::Bytes(b"second_variant".to_vec())
    );
    assert_eq!(
        Value::from(SomeTypeIsString::ThirdVariant),
        Value::Bytes(b"third_variant".to_vec())
    );
}

#[test]
fn from_value_is_integer() {
    use crate::{prelude::FromValue, Value};
    #[derive(FromValue, Debug, Eq, PartialEq)]
    #[mysql(is_integer, rename_all = "snake_case")]
    #[repr(i8)]
    enum SomeTypeIsInteger {
        FirstVariant = -1_i8,
        SecondVariant = 2,
        ThirdVariant = 3,
    }

    let value = Value::Int(-1);
    assert_eq!(
        SomeTypeIsInteger::FirstVariant,
        SomeTypeIsInteger::from_value(value)
    );

    let value = Value::Int(3);
    assert_eq!(
        SomeTypeIsInteger::ThirdVariant,
        SomeTypeIsInteger::from_value(value)
    );

    assert_eq!(Value::from(SomeTypeIsInteger::FirstVariant), Value::Int(-1));
    assert_eq!(Value::from(SomeTypeIsInteger::SecondVariant), Value::Int(2));
    assert_eq!(Value::from(SomeTypeIsInteger::ThirdVariant), Value::Int(3));
}

#[cfg(test)]
mod tests {
    use crate::{
        constants::ColumnType,
        packets::Column,
        row::{convert::FromRow, new_row},
        value::{convert::from_value, Value},
        FromValueError,
    };
    use unic_langid::LanguageIdentifier;

    #[derive(FromValue)]
    #[mysql(serialize_with = "from_langid", deserialize_with = "to_langid")]
    struct LangId(LanguageIdentifier);

    impl std::ops::Deref for LangId {
        type Target = LanguageIdentifier;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    fn to_langid(v: Value) -> Result<LanguageIdentifier, FromValueError> {
        match v {
            Value::Bytes(ref b) => match LanguageIdentifier::from_bytes(b) {
                Ok(ident) => Ok(ident),
                Err(_) => Err(FromValueError(v)),
            },
            _ => Err(FromValueError(v)),
        }
    }

    fn from_langid(land_id: LanguageIdentifier) -> Value {
        Value::Bytes(land_id.to_string().into())
    }

    #[test]
    fn newtype_with() {
        let mut value = Value::Bytes(b"en-US".into());

        let ident = from_value::<LangId>(value);

        assert_eq!(ident.language.to_string().as_str(), "en");
        assert_eq!(ident.to_string().as_str(), "en-US");

        value = ident.into();

        assert_eq!(value, Value::Bytes(b"en-US".into()));
    }

    #[test]
    fn from_row_derive() {
        #[derive(FromRow)]
        #[mysql(table_name = "Foos", rename_all = "camelCase")]
        struct Foo {
            id: u64,
            text_data: String,
            #[mysql(json)]
            json_data: serde_json::Value,
            #[mysql(with = "from_literal", rename = "custom")]
            custom_bool: bool,
        }

        fn from_literal(value: crate::Value) -> Result<bool, crate::FromValueError> {
            match value {
                crate::Value::Bytes(x) if x == b"true" => Ok(true),
                crate::Value::Bytes(x) if x == b"false" => Ok(false),
                x => Err(crate::FromValueError(x)),
            }
        }

        assert_eq!(Foo::TABLE_NAME, "Foos");
        assert_eq!(Foo::ID_FIELD, "id");
        assert_eq!(Foo::TEXT_DATA_FIELD, "textData");
        assert_eq!(Foo::JSON_DATA_FIELD, "jsonData");
        assert_eq!(Foo::CUSTOM_BOOL_FIELD, "custom");

        let columns = vec![
            Column::new(ColumnType::MYSQL_TYPE_LONGLONG)
                .with_name(b"id")
                .with_org_name(b"id")
                .with_table(b"Foos")
                .with_org_table(b"Foos"),
            Column::new(ColumnType::MYSQL_TYPE_VARCHAR)
                .with_name(b"textData")
                .with_org_name(b"textData")
                .with_table(b"Foos")
                .with_org_table(b"Foos"),
            Column::new(ColumnType::MYSQL_TYPE_JSON)
                .with_name(b"jsonData")
                .with_org_name(b"jsonData")
                .with_table(b"Foos")
                .with_org_table(b"Foos"),
            Column::new(ColumnType::MYSQL_TYPE_VARCHAR)
                .with_name(b"custom")
                .with_org_name(b"custom")
                .with_table(b"Foos")
                .with_org_table(b"Foos"),
        ];

        let row = new_row(
            vec![
                crate::Value::Int(10),
                crate::Value::Bytes(b"bytes".into()),
                crate::Value::Bytes(b"[true,false,\"not found\"]".into()),
                crate::Value::Bytes(b"true".into()),
            ],
            columns.into(),
        );

        let deserialized = Foo::from_row(row);

        assert_eq!(deserialized.id, 10);
        assert_eq!(deserialized.text_data, "bytes");
        assert_eq!(
            deserialized.json_data.to_string(),
            "[true,false,\"not found\"]"
        );
        assert!(deserialized.custom_bool);
    }
}
