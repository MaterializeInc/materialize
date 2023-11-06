// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

//! Shared types for the `mz-stash*` crates

use std::error::Error;
use std::fmt::{self, Debug};

use mz_proto::TryFromProtoError;
use tokio_postgres::error::SqlState;

pub mod metrics;
pub mod objects;

/// Not a public API, only exposed for mz-stash.
pub mod upgrade {
    use paste::paste;
    macro_rules! objects {
        ( $( $x:ident ),* ) => {
            paste! {
                $(
                    pub mod [<objects_ $x>] {
                        include!(concat!(env!("OUT_DIR"), "/objects_", stringify!($x), ".rs"));
                    }
                )*
            }
        }
    }

    objects!(v35, v36, v37, v38, v39, v40, v41, v42);
}

/// The current version of the `Stash`.
///
/// We will initialize new `Stash`es with this version, and migrate existing `Stash`es to this
/// version. Whenever the `Stash` changes, e.g. the protobufs we serialize in the `Stash`
/// change, we need to bump this version.
pub const STASH_VERSION: u64 = 42;

/// The minimum `Stash` version number that we support migrating from.
///
/// After bumping this we can delete the old migrations.
pub const MIN_STASH_VERSION: u64 = 35;

/// An error that can occur while interacting with a `Stash`.
///
/// Stash errors are deliberately opaque. They generally indicate unrecoverable
/// conditions, like running out of disk space.
#[derive(Debug)]
pub struct StashError {
    /// Not a public API, only exposed for mz-stash.
    pub inner: InternalStashError,
}

impl StashError {
    /// Reports whether the error is unrecoverable (retrying will never succeed,
    /// or a retry is not safe due to an indeterminate state).
    pub fn is_unrecoverable(&self) -> bool {
        match &self.inner {
            InternalStashError::Fence(_) | InternalStashError::StashNotWritable(_) => true,
            _ => false,
        }
    }

    /// Reports whether the error can be recovered if we opened the stash in writeable
    pub fn can_recover_with_write_mode(&self) -> bool {
        match &self.inner {
            InternalStashError::StashNotWritable(_) => true,
            _ => false,
        }
    }

    /// The underlying transaction failed in a way that must be resolved by retrying
    pub fn should_retry(&self) -> bool {
        match &self.inner {
            InternalStashError::Postgres(e) => {
                matches!(e.code(), Some(&SqlState::T_R_SERIALIZATION_FAILURE))
            }
            _ => false,
        }
    }
}

/// Not a public API, only exposed for mz-stash.
#[derive(Debug)]
pub enum InternalStashError {
    Postgres(::tokio_postgres::Error),
    Fence(String),
    PeekSinceUpper(String),
    IncompatibleVersion(u64),
    Proto(TryFromProtoError),
    Decoding(prost::DecodeError),
    Uninitialized,
    StashNotWritable(String),
    Other(String),
}

impl fmt::Display for StashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("stash error: ")?;
        match &self.inner {
            InternalStashError::Postgres(e) => write!(f, "postgres: {e}"),
            InternalStashError::Proto(e) => write!(f, "proto: {e}"),
            InternalStashError::Decoding(e) => write!(f, "prost decoding: {e}"),
            InternalStashError::Fence(e) => f.write_str(e),
            InternalStashError::PeekSinceUpper(e) => f.write_str(e),
            InternalStashError::IncompatibleVersion(v) => {
                write!(f, "incompatible Stash version {v}, minimum: {MIN_STASH_VERSION}, current: {STASH_VERSION}")
            }
            InternalStashError::Uninitialized => write!(f, "uninitialized"),
            InternalStashError::StashNotWritable(e) => f.write_str(e),
            InternalStashError::Other(e) => f.write_str(e),
        }
    }
}

impl Error for StashError {}

impl From<InternalStashError> for StashError {
    fn from(inner: InternalStashError) -> StashError {
        StashError { inner }
    }
}

impl From<prost::DecodeError> for StashError {
    fn from(e: prost::DecodeError) -> Self {
        StashError {
            inner: InternalStashError::Decoding(e),
        }
    }
}

impl From<TryFromProtoError> for StashError {
    fn from(e: TryFromProtoError) -> Self {
        StashError {
            inner: InternalStashError::Proto(e),
        }
    }
}

impl From<String> for StashError {
    fn from(e: String) -> StashError {
        StashError {
            inner: InternalStashError::Other(e),
        }
    }
}

impl From<&str> for StashError {
    fn from(e: &str) -> StashError {
        StashError {
            inner: InternalStashError::Other(e.into()),
        }
    }
}

impl From<std::io::Error> for StashError {
    fn from(e: std::io::Error) -> StashError {
        StashError {
            inner: InternalStashError::Other(e.to_string()),
        }
    }
}

impl From<anyhow::Error> for StashError {
    fn from(e: anyhow::Error) -> Self {
        StashError {
            inner: InternalStashError::Other(e.to_string()),
        }
    }
}

impl From<tokio_postgres::Error> for StashError {
    fn from(e: tokio_postgres::Error) -> StashError {
        StashError {
            inner: InternalStashError::Postgres(e),
        }
    }
}
