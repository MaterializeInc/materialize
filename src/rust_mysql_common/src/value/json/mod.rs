// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use serde::{Deserialize, Serialize};

pub mod serde_integration;

/// Use it to pass `T: Serialize` as JSON to a prepared statement.
///
/// ```ignore
/// #[derive(Serialize)]
/// struct SerializableStruct {
///     // ...
/// }
///
/// conn.prep_exec("INSERT INTO table (json_column) VALUES (?)",
///                (Serialized(SerializableStruct),));
/// ```
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash, Serialize)]
#[serde(transparent)]
pub struct Serialized<T>(pub T);

/// Use it to parse `T: Deserialize` from `Value`.
///
/// ```ignore
/// #[derive(Deserialize)]
/// struct DeserializableStruct {
///     // ...
/// }
/// // ...
/// let (Deserialized(val),): (Deserialized<DeserializableStruct>,)
///     = from_row(row_with_single_json_column);
/// ```
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash, Deserialize)]
#[serde(transparent)]
pub struct Deserialized<T>(pub T);
