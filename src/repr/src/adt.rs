// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstract data types.
//!
//! Native Rust types are used for many primitive types, but many of the more
//! complicated types require custom implementations, which are contained in
//! this module.
//!
//! Where possible, these implementations match the [PostgreSQL ADTs].
//!
//! [PostgreSQL ADTs]: https://github.com/postgres/postgres/tree/master/src/backend/utils/adt

pub mod array;
pub mod char;
pub mod date;
pub mod datetime;
pub mod interval;
pub mod jsonb;
pub mod numeric;
pub mod regex;
pub mod system;
pub mod varchar;
