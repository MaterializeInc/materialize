// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Internal utility libraries for Materialize.
//!
//! **ore** (_n_): the raw material from which more valuable materials are extracted.
//! Modules are included in this crate when they are broadly useful but too
//! small to warrant their own crate.

#![deny(missing_docs, missing_debug_implementations)]

pub mod cast;
pub mod collections;
pub mod future;
pub mod hash;
pub mod iter;
pub mod log;
pub mod netio;
pub mod option;
pub mod panic;
pub mod stats;
pub mod sync;
pub mod thread;
pub mod tokio;

/// Logs a message to stderr and crashes the process.
///
/// TODO(benesch): examples.
#[macro_export]
macro_rules! fatal {
    ($($arg:tt)*) => {{
        if cfg!(test) {
            panic!($($arg)*);
        } else {
            use log::error;
            error!($($arg)*);
            std::process::exit(1);
        }
    }}
}

/// Defines a closure that can easily clone variables.
///
/// TODO(benesch): examples.
#[macro_export]
macro_rules! closure {
    ([$($lvar:tt)*] $($closure:tt)*) => (
        closure!(@ $($lvar)* @ move $($closure)*)
    );
    (@ , $($rest:tt)*) => (closure!(@ $($rest)*));
    (@ clone $lvar:ident $($rest:tt)*) => {{
        let $lvar = $lvar.clone();
        closure!(@ $($rest)*)
    }};
    (@ ref $lvar:ident $($rest:tt)*) => {{
        let $lvar = &$lvar;
        closure!(@ $($rest)*)
    }};
    (@ ref mut $lvar:ident $($rest:tt)*) => {{
        let $lvar = &mut $lvar;
        closure!(@ $($rest)*)
    }};
    (@ @ $bl:expr) => {{
        $bl
    }};
}
