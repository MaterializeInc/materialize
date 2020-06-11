// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module houses the definitions of the structs and enums that are
//! formally part of the AST.
//!
//! A few notes about the layout of this module:
//!
//!   * The public interface of the `sql::ast` module exposes all of these types
//!     in one big namespace.
//!
//!     The submodules here exist only to avoid having one massive file with
//!     every type in it. Separation into modules would be annoying for
//!     consumers of the AST, who don't want to remember e.g. whether the
//!     `Ident` type is in the `expr` module or the `name` module or the `value`
//!     module.
//!
//!   * Only types that are actually part of the AST live in this module. This
//!     module's contents are parsed by the build script to e.g. automatically
//!     generate an AST visitor, and the build script gets confused if there are
//!     helper structs interspersed.
//!
//!     Helper structs should live in a separate submodule of `crate::ast`, like
//!     `crate::ast::display`.
//!
//!   * Do *not* import types from this module directly. I.e., use
//!     `crate::ast::Ident`, not `crate::ast::defs::name::Ident`. The former
//!     import path makes it possible to reorganize this module without changing
//!     every `use` statement.

mod ddl;
mod expr;
mod name;
mod operator;
mod query;
mod statement;
mod value;

pub use ddl::*;
pub use expr::*;
pub use name::*;
pub use operator::*;
pub use query::*;
pub use statement::*;
pub use value::*;
