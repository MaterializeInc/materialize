// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

/// Denotes that `Self` is wire compatible with type `T`.
///
/// You should not implement this yourself, instead use the `wire_compatible!` macro.
pub unsafe trait WireCompatible<T: prost::Message>: prost::Message + Default {
    /// Converts the type `T` into `Self` by serializing `T` and deserializing as `Self`.
    fn convert(old: &T) -> Self {
        let bytes = old.encode_to_vec();
        // Note: use Bytes to enable possible re-use of the underlying buffer.
        let bytes = Bytes::from(bytes);
        Self::decode(bytes).expect("wire compatible")
    }
}

// SAFETY: A message type is trivially wire compatible with itself.
unsafe impl<T: prost::Message + Default + Clone> WireCompatible<T> for T {
    fn convert(old: &Self) -> Self {
        old.clone()
    }
}

/// Defines one protobuf type as wire compatible with another.
///
/// ```text
/// wire_compatible!(objects_v28::DatabaseKey with objects_v27::DatabaseKey);
/// ```
///
/// Internally this will implement the `WireCompatible<B> for <A>`, e.g.
/// `WireCompatible<objects_v27::DatabaseKey> for objects_v28::DatabaseKey` and generate `proptest`
/// cases that will create arbitrary objects of type `B` and assert they can be deserialized with
/// type `A`, and vice versa.
#[macro_export]
macro_rules! wire_compatible {
    ($a:ident $(:: $a_sub:ident)* with $b:ident $(:: $b_sub:ident)*) => {
        ::static_assertions::assert_impl_all!(
            $a $(::$a_sub)* : ::proptest::arbitrary::Arbitrary, ::prost::Message, Default,
        );
        ::static_assertions::assert_impl_all!(
            $b $(::$b_sub)*  : ::proptest::arbitrary::Arbitrary, ::prost::Message, Default,
        );

        // SAFETY: Below we assert that these types are wire compatible by generating arbitrary
        // structs, encoding in one, and then decoding in the other.
        unsafe impl $crate::durable::upgrade::wire_compatible::WireCompatible< $b $(::$b_sub)* > for $a $(::$a_sub)* {}
        unsafe impl $crate::durable::upgrade::wire_compatible::WireCompatible< $a $(::$a_sub)* > for $b $(::$b_sub)* {}

        ::paste::paste! {
            ::proptest::proptest! {
                #![proptest_config(::proptest::test_runner::Config {
                    cases: 64,
                    ..Default::default()
                })]

                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_wire_compat_ $a:snake $(_$a_sub:snake)* _to_ $b:snake $(_$b_sub:snake)* >](a: $a $(::$a_sub)* ) {
                    use ::prost::Message;
                    let a_bytes = a.encode_to_vec();
                    let b_decoded = $b $(::$b_sub)*::decode(&a_bytes[..]);
                    ::proptest::prelude::prop_assert!(b_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let b_decoded = b_decoded.expect("asserted Ok");
                    let b_converted: $b $(::$b_sub)* = $crate::durable::upgrade::wire_compatible::WireCompatible::convert(&a);
                    assert_eq!(b_decoded, b_converted);

                    let b_bytes = b_decoded.encode_to_vec();
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }

                #[mz_ore::test]
                #[cfg_attr(miri, ignore)] // slow
                fn [<proptest_wire_compat_ $b:snake $(_$b_sub:snake)* _to_ $a:snake $(_$a_sub:snake)* >](b: $b $(::$b_sub)* ) {
                    use ::prost::Message;
                    let b_bytes = b.encode_to_vec();
                    let a_decoded = $a $(::$a_sub)*::decode(&b_bytes[..]);
                    ::proptest::prelude::prop_assert!(a_decoded.is_ok());

                    // Maybe superfluous, but this is a method called in production.
                    let a_decoded = a_decoded.expect("asserted Ok");
                    let a_converted: $a $(::$a_sub)* = $crate::durable::upgrade::wire_compatible::WireCompatible::convert(&b);
                    assert_eq!(a_decoded, a_converted);

                    let a_bytes = a_decoded.encode_to_vec();
                    ::proptest::prelude::prop_assert_eq!(a_bytes, b_bytes, "a and b serialize differently");
                }
            }
        }
    };
}
pub use wire_compatible;
