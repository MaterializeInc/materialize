// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

/// It's a wrapper for `bitflags::bitflags!` macro that also implements the `Bitflags` trait.
macro_rules! my_bitflags {
    ($name:ident, $(#[$em:meta])+ $err:ident, $ty:path, $($def:tt)*) => {
        #[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, thiserror::Error)]
        $(#[$em])+
        pub struct $err(pub $ty);

        impl TryFrom<$ty> for $name {
            type Error = $err;

            fn try_from(val: $ty) -> std::result::Result<$name, $err> {
                $name::from_bits(val).ok_or_else(|| $err(val))
            }
        }

        impl From<$name> for $ty {
            fn from(val: $name) -> $ty {
                val.bits()
            }
        }

        impl Default for $name {
            fn default() -> $name {
                $name::empty()
            }
        }

        bitflags::bitflags! { $($def)* }
    };
}
