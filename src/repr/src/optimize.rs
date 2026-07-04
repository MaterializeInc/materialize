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

#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// A macro for feature flags managed by the optimizer.
macro_rules! optimizer_feature_flags {
    ({ $($feature:ident: $type:ty,)* }) => {
        #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
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
    // Use `EquivalenceClassesWithholdingErrors` instead of raw
    // `EquivalenceClasses` during eq prop for joins.
    enable_eq_classes_withholding_errors: bool,
    // Bound from `SystemVars::enable_eager_delta_joins`.
    enable_eager_delta_joins: bool,
    // Run the equality-saturation MIR optimizer pass (mz_transform::eqsat).
    // Bound from `SystemVars::enable_eqsat_optimizer`.
    enable_eqsat_optimizer: bool,
    // Run the physical eqsat placement that commits WcoJoin to DeltaQuery.
    // Bound from `SystemVars::enable_eqsat_physical_optimizer`.
    // Default off: the placement is a feasibility spike; flag-on validation
    // against the full SLT corpus gates promotion.
    enable_eqsat_physical_optimizer: bool,
    // Enable Lattice-based fixpoint iteration on LetRec nodes in the
    // Analysis framework.
    enable_letrec_fixpoint_analysis: bool,
    // Bound from `SystemVars::enable_new_outer_join_lowering`.
    enable_new_outer_join_lowering: bool,
    // Bound from `SystemVars::enable_reduce_mfp_fusion`.
    enable_reduce_mfp_fusion: bool,
    // Enable joint HIR ⇒ MIR lowering of stacks of left joins.
    enable_variadic_left_join_lowering: bool,
    // Enable cardinality estimation
    enable_cardinality_estimates: bool,
    // An exclusive upper bound on the number of results we may return from a
    // Persist fast-path peek. Required by the `create_fast_path_plan` call in
    // `peek::Optimizer`.
    persist_fast_path_limit: usize,
    // Reoptimize imported views when building and optimizing a
    // `DataflowDescription` in the global MIR optimization phase.
    reoptimize_imported_views: bool,
    // See the feature flag of the same name.
    enable_join_prioritize_arranged: bool,
    // See the feature flag of the same name.
    enable_projection_pushdown_after_relation_cse: bool,
    // See the feature flag of the same name.
    enable_less_reduce_in_eqprop: bool,
    // See the feature flag of the same name.
    enable_dequadratic_eqprop_map: bool,
    // See the feature flag of the same name.
    enable_fast_path_plan_insights: bool,
    // See the feature flag of the same name.
    enable_cast_elimination: bool,
    // See the feature flag of the same name.
    enable_case_literal_transform: bool,
    // See the feature flag of the same name.
    enable_simplify_quantified_comparisons: bool,
    // See the feature flag of the same name.
    enable_coalesce_case_transform: bool,
    // See the feature flag of the same name.
    enable_will_distinct_propagation: bool,
    // Use the ILP extractor (0/1 program over arrangement count) instead of the
    // greedy bottom-up extractor. Default on. Set to false to fall back to the
    // greedy bottom-up extractor for debugging or A/B comparison.
    enable_eqsat_ilp_extraction: bool,
    // Tag LetRec local references with a RecVersion so the eqsat engine can
    // lift and share subexpressions across WMR bindings. Default off.
    // Bound from `SystemVars::enable_eqsat_wmr_lift`.
    enable_eqsat_wmr_lift: bool,
    // Use the equality-saturation scalar canonicalizer in place of `reduce`
    // inside `canonicalize_predicates`. Default on.
    // Bound from `SystemVars::enable_eqsat_scalar_canonicalize`.
    enable_eqsat_scalar_canonicalize: bool,
    // Bound from `SystemVars::enable_eqsat_delta_join_cost`. Gates the
    // delta-aware join-cost spelling selector in eqsat extraction.
    enable_eqsat_delta_join_cost: bool,
    // Bound from `SystemVars::enable_eqsat_native_join_commit`. Commit eqsat
    // acyclic joins to a Differential at raise time so JoinImplementation
    // no-ops on them.
    enable_eqsat_native_join_commit: bool,
    // Bound from `SystemVars::enable_eqsat_filter_sharing`. Gates the eqsat
    // filter-split rewrite and the scalar-aware ILP node tier. Default off.
    enable_eqsat_filter_sharing: bool,
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
impl_optimizer_feature_type![usize];

impl OptimizerFeatureType for bool {
    fn encode(self) -> String {
        self.to_string()
    }

    fn decode(v: &str) -> Self {
        // Accept both the Rust spelling (`true`/`false`, what `encode` and
        // `CREATE CLUSTER ... FEATURES` produce) and the canonical system-var /
        // PostgreSQL spellings (`on`/`off`, ...). A cluster-coherent scoped
        // override may arrive as the var's canonical `on`/`off`.
        match v.trim().to_lowercase().as_str() {
            "t" | "true" | "on" | "1" | "y" | "yes" => true,
            "f" | "false" | "off" | "0" | "n" | "no" => false,
            // The writer only stores values that passed `canonicalize`, so this
            // arm is unreachable in practice. Log rather than panic anyway: a
            // stored bad value reaching the plan path would otherwise crash the
            // optimizer on every query for the affected cluster. Fall back to
            // `false`.
            other => {
                mz_ore::soft_panic_or_log!("invalid boolean optimizer feature override: {other:?}");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::OptimizerFeatureOverrides;

    #[mz_ore::test]
    fn bool_override_decodes_lenient_spellings() {
        for (stored, want) in [
            ("true", true),
            ("false", false),
            ("on", true),
            ("off", false),
        ] {
            let map =
                BTreeMap::from([("enable_eager_delta_joins".to_string(), stored.to_string())]);
            assert_eq!(
                OptimizerFeatureOverrides::from(map).enable_eager_delta_joins,
                Some(want),
            );
        }
    }
}
