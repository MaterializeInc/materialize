// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The compiled rewrite rules, generated at build time from
//! `eqsat/rules/relational.rewrite`.
//!
//! `build.rs` parses the rule file with a [`chumsky`](https://crates.io/crates/chumsky)
//! grammar and emits this module's body into `$OUT_DIR/eqsat_rules.rs`. Nothing
//! parses the rule file at run time: each rule becomes a pair of generated
//! functions, a `find` that enumerates left-hand-side matches (checking side
//! conditions) and an `apply` that instantiates the right-hand side. The
//! generated `rules_ast()` reconstructs the rule set as AST literals for the
//! Lean emitter (`super::lean`).

// The generated matchers favor uniformity over the lints the rest of the crate
// observes; they are machine-written, not hand-tuned.
#![allow(clippy::all, unused)]

use crate::eqsat::dsl::Phase;
use crate::eqsat::egraph::view::{ApplyGraph, MatchGraph};
use crate::eqsat::egraph::{
    Analyses, CNode, EBindings, EGraph, ENode, Id, Index, Sym, cond_all_columns, cond_all_true,
    cond_any_false, cond_cols_in_range, cond_identity_projection, cond_no_error, cond_no_false,
    cond_uses_only_input,
};
use crate::eqsat::matcher::{
    Payload, cols_of_payload, compose_payload, concat_payload, equivs_inner, equivs_outer,
    iota_payload, remap_payload, shift_payload, swap_equivs, swap_projection,
};

/// Which e-class analyses a rule's side conditions read. The saturation loop
/// recomputes an analysis each round only when some active rule needs it, so a
/// rule set that never reads, say, the monotonicity analysis does not pay to
/// recompute it. Derived at build time from each rule's conditions (see
/// `build/codegen.rs`), so it stays correct as rules are added.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AnalysisNeeds {
    /// Reads the non-negativity analysis (`Cond::NonNegative`).
    pub nonneg: bool,
    /// Reads the unique-key analysis (`Cond::IsUniqueKey`).
    pub keys: bool,
    /// Reads the monotonicity analysis (`Cond::Monotonic`).
    pub monotonic: bool,
}

/// One rewrite rule, compiled to a matcher (`find`) and an instantiator
/// (`apply`). Both are functions generated from `relational.rewrite`.
#[derive(Clone, Copy)]
pub struct CompiledRule {
    /// The rule's name (for `rule_names` and diagnostics).
    pub name: &'static str,
    /// The eqsat pass(es) the rule is active in.
    pub phase: Phase,
    /// Which e-class analyses this rule's conditions read.
    pub(crate) needs: AnalysisNeeds,
    /// Whether this rule is safe to apply in the "colored saturation" phase.
    /// A colored rule uses only color-exact side conditions (no analysis-gated
    /// conditions: no `non_negative`, `monotonic`, or `is_unique_key`).
    /// Enforced at build time by a `cond_is_color_exact` assertion in codegen.
    pub(crate) colored: bool,
    /// Enumerate every left-hand-side match in the e-graph that also satisfies
    /// the side conditions, up to `limit` matches. The `bool` is `true` when
    /// the cap was reached (so the caller can throttle an explosive rule).
    pub(crate) find:
        fn(&crate::eqsat::egraph::view::BaseView, &Analyses, usize) -> (Vec<EBindings>, bool),
    /// Instantiate the right-hand side for a match, returning the new e-class.
    pub(crate) apply: fn(&mut EGraph, &EBindings) -> Result<Id, String>,
}

impl std::fmt::Debug for CompiledRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledRule")
            .field("name", &self.name)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

/// A set of compiled rules, the run-time replacement for the former AST
/// `dsl::RuleSet`.
#[derive(Clone, Debug)]
pub struct CompiledRuleSet {
    rules: Vec<&'static CompiledRule>,
}

impl CompiledRuleSet {
    /// The compiled rules, in declaration order.
    pub(crate) fn rules(&self) -> &[&'static CompiledRule] {
        &self.rules
    }

    /// The name of every rule in this set, in order.
    pub fn rule_names(&self) -> Vec<&'static str> {
        self.rules.iter().map(|r| r.name).collect()
    }

    /// The union of analyses read by any rule in this set. The saturation loop
    /// recomputes an analysis each round only when this reports it is needed.
    pub(crate) fn needed_analyses(&self) -> AnalysisNeeds {
        self.rules
            .iter()
            .fold(AnalysisNeeds::default(), |acc, r| AnalysisNeeds {
                nonneg: acc.nonneg || r.needs.nonneg,
                keys: acc.keys || r.needs.keys,
                monotonic: acc.monotonic || r.needs.monotonic,
            })
    }

    /// The rules tagged `colored`: those whose side conditions are all
    /// color-exact (no analysis-gated conditions). Suitable for the colored
    /// saturation phase. The build-time assertion in `codegen.rs` guarantees
    /// that every rule in this set uses only color-exact conditions.
    pub(crate) fn colored_rules(&self) -> Vec<&'static CompiledRule> {
        self.rules.iter().copied().filter(|r| r.colored).collect()
    }

    /// Append one hand-written rule that lives outside the generated
    /// `COMPILED_RULES` table (for example `filter_split`, which the DSL cannot
    /// express). The rule must be a `&'static`, matching the set's element type,
    /// so it is registered as a `static` with its cap baked in rather than built
    /// per call.
    pub(crate) fn with_extra_rule(mut self, rule: &'static CompiledRule) -> CompiledRuleSet {
        self.rules.push(rule);
        self
    }

    /// A set of exactly the given `&'static` rules, in order. Used by unit tests
    /// of hand-written rules to saturate with a single rule.
    #[cfg(test)]
    pub(crate) fn of(rules: Vec<&'static CompiledRule>) -> CompiledRuleSet {
        CompiledRuleSet { rules }
    }

    /// The rules active in `phase`: those declared for that phase or for `Both`.
    pub fn for_phase(&self, phase: Phase) -> CompiledRuleSet {
        CompiledRuleSet {
            rules: self
                .rules
                .iter()
                .copied()
                .filter(|r| r.phase == Phase::Both || r.phase == phase)
                .collect(),
        }
    }
}

/// The full built-in rule set.
pub fn all() -> CompiledRuleSet {
    CompiledRuleSet {
        rules: COMPILED_RULES.iter().collect(),
    }
}

/// The scalar-sort rewrite rules, run only by the scalar canonicalizer
/// (`crate::eqsat::scalar_saturate`) and never in the relational saturation pass.
/// Backed by the generated `SCALAR_COMPILED_RULES` table.
pub(crate) fn scalar_all() -> CompiledRuleSet {
    CompiledRuleSet {
        rules: SCALAR_COMPILED_RULES.iter().collect(),
    }
}

include!(concat!(env!("OUT_DIR"), "/eqsat_rules.rs"));

#[cfg(test)]
mod tests {
    /// `colored_rules()` returns a tagged subset of all rules, containing the
    /// filter-movement rules but not analysis-gated rules.
    #[mz_ore::test]
    fn colored_rules_are_tagged_subset() {
        let set = super::all();
        let names: std::collections::BTreeSet<_> =
            set.colored_rules().iter().map(|r| r.name).collect();
        assert!(names.contains("drop_true_filter"));
        assert!(names.contains("merge_filters"));
        assert!(names.contains("push_filter_through_map"));
        // An analysis-gated rule must never be colored.
        assert!(!names.iter().any(|n| *n == "reduce_elision")); // uses is_unique_key
    }

    /// The two generated backends (compiled rules and the AST `rules_ast()`)
    /// come from the same source, so their rule names and phases must agree.
    /// `rules_ast()` carries every rule regardless of sort (it feeds the Lean
    /// emitter, which will translate scalar patterns starting in a later
    /// slice), so it is compared against `COMPILED_RULES` followed by
    /// `SCALAR_COMPILED_RULES`: build.rs appends `scalar.rewrite`'s rules after
    /// `relational.rewrite`'s and codegen's per-sort filters are stable, so
    /// that concatenation reproduces the original declaration order.
    #[mz_ore::test]
    fn compiled_and_ast_agree() {
        let ast = super::rules_ast();
        let ast_names: Vec<&str> = ast.rules.iter().map(|r| r.name.as_str()).collect();
        let compiled: Vec<&super::CompiledRule> = super::COMPILED_RULES
            .iter()
            .chain(super::SCALAR_COMPILED_RULES.iter())
            .collect();
        let compiled_names: Vec<&str> = compiled.iter().map(|r| r.name).collect();
        assert_eq!(compiled_names, ast_names);
        for (c, a) in compiled.iter().zip(&ast.rules) {
            assert_eq!(c.name, a.name);
            assert_eq!(c.phase, a.phase, "phase mismatch for {}", c.name);
        }
    }
}
