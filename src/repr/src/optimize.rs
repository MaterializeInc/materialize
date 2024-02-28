// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Facilities for defining optimizer feature flags.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A macro for feature flags managed by the optimizer.
macro_rules! optimizer_feature_flags {
    ({ $($feature:ident: $type:ty,)* }) => {
        #[derive(Clone, Debug)]
        pub struct OptimizerFeatures {
            $(pub $feature: $type),*
        }

        #[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
        pub struct OptimizerFeatureOverrides {
            $(pub $feature: Option<$type>),*
        }

        impl Default for OptimizerFeatureOverrides {
            fn default() -> Self {
                Self {
                    $($feature: None),*
                }
            }
        }


        /// Synthesize `OptimizerFeatureOverrides ⇒ BTreeMap<String, String>`
        /// conversion.
        ///
        /// WARNING: changing the definition of item might break catalog
        /// re-hydration for some catalog items (such as entries for `CREATE
        /// CLUSTER ... FEATURES(...)` statements).
        impl From<BTreeMap<String, String>> for OptimizerFeatureOverrides {
            fn from(value: BTreeMap<String, String>) -> Self {
                let mut overrides = OptimizerFeatureOverrides::default();

                for (name, value) in value.into_iter() {
                    match name.as_str() {
                        $(stringify!($feature) => {
                            let value = Some(<$type>::decode(&value));
                            overrides.$feature = value;
                        }),*
                        _ => (), // Ignore
                    }
                }

                overrides
            }
        }

        /// Synthesize `BTreeMap<String, String> ⇒ OptimizerFeatureOverrides`
        /// conversion.
        ///
        /// WARNING: changing the definition of item might break catalog
        /// re-hydration for some catalog items (such as entries for `CREATE
        /// CLUSTER ... FEATURES(...)` statements).
        impl From<OptimizerFeatureOverrides> for BTreeMap<String, String> {
            fn from(overrides: OptimizerFeatureOverrides) -> Self {
                let mut map = BTreeMap::<String, String>::default();

                $(if let Some(value) = overrides.$feature {
                    let k = stringify!($feature).into();
                    let v = value.encode();
                    map.insert(k, v);
                };)*

                map
            }
        }
    };
}

optimizer_feature_flags!({
    enable_consolidate_after_union_negate: bool,
    persist_fast_path_limit: usize,
    reoptimize_imported_views: bool,
    enable_new_outer_join_lowering: bool,
    enable_eager_delta_joins: bool,
    enable_reduce_mfp_fusion: bool,
});

/// A trait that handles conversion of feature flags.
trait OptimizerFeatureType {
    fn encode(self) -> String;
    fn decode(v: &str) -> Self;
}

/// A macro that implements [`OptimizerFeatureType`] for most common types.
///
/// WARNING: changing the definition of this macro might break catalog
/// re-hydration for some catalog items (such as entries for `CREATE CLUSTER ...
/// FEATURES(...)` statements).
macro_rules! impl_optimizer_feature_type {
    ($($type:ty),*) => {
        $(
            impl OptimizerFeatureType for $type {
                fn encode(self) -> String {
                    self.to_string()
                }

                fn decode(v: &str) -> Self {
                    str::parse(&v).unwrap()
                }
            }
        )*
    };
}

// Implement `OptimizerFeatureType` for all types used in the
// `optimizer_feature_flags!(...)`  call above.
impl_optimizer_feature_type![bool, usize];
