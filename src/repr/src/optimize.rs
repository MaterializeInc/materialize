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
        #[derive(Clone, Debug, Default)]
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

        /// An [`OverrideFrom`] implementation that updates
        /// [`OptimizerFeatures`] using [`OptimizerFeatureOverrides`] values.
        impl OverrideFrom<OptimizerFeatureOverrides> for OptimizerFeatures {
            fn override_from(mut self, overrides: &OptimizerFeatureOverrides) -> Self {
                $(if let Some(feature_value) = overrides.$feature {
                    self.$feature = feature_value;
                })*
                self
            }
        }

        /// An `OptimizerFeatureOverrides ⇒ BTreeMap<String, String>`
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

        /// A `BTreeMap<String, String> ⇒ OptimizerFeatureOverrides` conversion.
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
    // Enable consolidation of unions that happen immediately after negate.
    //
    // The refinement happens in the LIR ⇒ LIR phase.
    enable_consolidate_after_union_negate: bool,
    // Bound from `SystemVars::enable_eager_delta_joins`.
    enable_eager_delta_joins: bool,
    // Enable Lattice-based fixpoint iteration on LetRec nodes in the
    // Analysis framework.
    enable_letrec_fixpoint_analysis: bool,
    // Bound from `SystemVars::enable_new_outer_join_lowering`.
    enable_new_outer_join_lowering: bool,
    // Bound from `SystemVars::enable_reduce_mfp_fusion`.
    enable_reduce_mfp_fusion: bool,
    // Enable joint HIR ⇒ MIR lowering of stacks of left joins.
    enable_variadic_left_join_lowering: bool,
    // Enable the extra null filter implemented in #28018.
    enable_outer_join_null_filter: bool,
    // Enable cardinality estimation
    enable_cardinality_estimates: bool,
    // An exclusive upper bound on the number of results we may return from a
    // Persist fast-path peek. Required by the `create_fast_path_plan` call in
    // `peek::Optimizer`.
    persist_fast_path_limit: usize,
    // Reoptimize imported views when building and optimizing a
    // `DataflowDescription` in the global MIR optimization phase.
    reoptimize_imported_views: bool,
    // Enables the value window function fusion optimization.
    enable_value_window_function_fusion: bool,
});

/// A trait used to implement layered config construction.
pub trait OverrideFrom<T> {
    /// Override the configuration represented by [`Self`] with values
    /// from the given `layer`.
    fn override_from(self, layer: &T) -> Self;
}

/// Overrides for `U` coming from an optional `T`.
impl<T, U> OverrideFrom<Option<&T>> for U
where
    Self: OverrideFrom<T>,
{
    fn override_from(self, layer: &Option<&T>) -> Self {
        match layer {
            Some(layer) => self.override_from(layer),
            None => self,
        }
    }
}

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
