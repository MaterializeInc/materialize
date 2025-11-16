// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Denotes that `Self` is JSON compatible with type `T`.
///
/// You should not implement this yourself, instead use the `json_compatible!` macro.
pub unsafe trait JsonCompatible<T>: Serialize + DeserializeOwned
where
    T: Serialize + DeserializeOwned,
{
    /// Converts the type `T` into `Self` by serializing `T` and deserializing as `Self`.
    fn convert(old: &T) -> Self {
        let bytes = serde_json::to_vec(old).expect("JSON serializable");
        serde_json::from_slice(&bytes).expect("JSON compatible")
    }
}

// SAFETY: A type is trivially JSON compatible with itself.
unsafe impl<T: Serialize + DeserializeOwned + Clone> JsonCompatible<T> for T {
    fn convert(old: &Self) -> Self {
        old.clone()
    }
}

/// Defines one type as JSON compatible with another.
///
/// ```text
/// json_compatible!(objects_v28::DatabaseKey with objects_v27::DatabaseKey);
/// ```
///
/// Internally this will implement `JsonCompatible<B> for <A>`, e.g.
/// `JsonCompatible<objects_v27::DatabaseKey> for objects_v28::DatabaseKey` and generate `proptest`
/// cases that will create arbitrary objects of type `B` and assert they can be deserialized with
/// type `A`, and vice versa.
#[macro_export]
macro_rules! json_compatible {
    ($a:ident $(:: $a_sub:ident)* with $b:ident $(:: $b_sub:ident)*) => {
        ::static_assertions::assert_impl_all!(
            $a $(::$a_sub)* : ::proptest::arbitrary::Arbitrary, ::serde::Serialize, ::serde::de::DeserializeOwned,
        );
        ::static_assertions::assert_impl_all!(
            $b $(::$b_sub)*  : ::proptest::arbitrary::Arbitrary, ::serde::Serialize, ::serde::de::DeserializeOwned,
        );

        // SAFETY: Below we assert that these types are JSON compatible by generating arbitrary
        // structs, encoding in one, and then decoding in the other.
        unsafe impl $crate::durable::upgrade::json_compatible::JsonCompatible< $b $(::$b_sub)* > for $a $(::$a_sub)* {}
        unsafe impl $crate::durable::upgrade::json_compatible::JsonCompatible< $a $(::$a_sub)* > for $b $(::$b_sub)* {}

        ::paste::paste! {
            ::proptest::proptest! {
                #![proptest_config(::proptest::test_runner::Config {
                    cases: 64,
                    ..Default::default()
                })]

                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_json_compat_ $a:snake $(_$a_sub:snake)* _to_ $b:snake $(_$b_sub:snake)* >](a: $a $(::$a_sub)* ) {
                    let a_bytes = ::serde_json::to_vec(&a).expect("JSON serializable");
                    let b_decoded = ::serde_json::from_slice::<$b $(::$b_sub)*>(&a_bytes);
                    ::proptest::prelude::prop_assert!(b_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let b_decoded = b_decoded.expect("asserted Ok");
                    let b_converted: $b $(::$b_sub)* = $crate::durable::upgrade::json_compatible::JsonCompatible::convert(&a);
                    assert_eq!(b_decoded, b_converted);

                    let b_bytes = ::serde_json::to_vec(&b_decoded).expect("JSON serializable");
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }

                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_json_compat_ $b:snake $(_$b_sub:snake)* _to_ $a:snake $(_$a_sub:snake)* >](b: $b $(::$b_sub)* ) {
                    let b_bytes = ::serde_json::to_vec(&b).expect("JSON serializable");
                    let a_decoded = ::serde_json::from_slice::<$a $(::$a_sub)*>(&b_bytes);
                    ::proptest::prelude::prop_assert!(a_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let a_decoded = a_decoded.expect("asserted Ok");
                    let a_converted: $a $(::$a_sub)* = $crate::durable::upgrade::json_compatible::JsonCompatible::convert(&b);
                    assert_eq!(a_decoded, a_converted);

                    let a_bytes = ::serde_json::to_vec(&a_decoded).expect("JSON serializable");
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }
            }
        }
    };
}
pub use json_compatible;
