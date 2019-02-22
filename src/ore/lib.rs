// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! Internal utility libraries for Materialize.
//!
//! Ore: the raw material from which more valuable materials are extracted.
//! Modules are included in this crate when they are broadly useful but too
//! small to warrant their own crate.

pub mod future;
pub mod log;
pub mod netio;

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
