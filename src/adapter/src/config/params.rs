// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_adapter_types::dyncfgs::ENABLE_SCOPED_SYSTEM_PARAMETERS;
use mz_sql::session::vars::{ENABLE_LAUNCHDARKLY, SystemVars, Value, Var, VarInput};

/// A struct that defines the system parameters that should be synchronized
pub struct SynchronizedParameters {
    /// The backing `SystemVars` instance. Synchronized parameters are exactly
    /// those that are returned by [SystemVars::iter_synced].
    system_vars: SystemVars,
    /// A set of names identifying the synchronized variables from the above
    /// `system_vars`.
    ///
    /// Derived from the above at construction time with the assumption that this
    /// set cannot change during the lifecycle of the [SystemVars] instance.
    synchronized: BTreeSet<&'static str>,
    /// A set of names that identifies the synchronized parameters that have been
    /// modified by the frontend and need to be pushed to backend.
    modified: BTreeSet<&'static str>,
}

impl Default for SynchronizedParameters {
    fn default() -> Self {
        Self::new(SystemVars::default())
    }
}

impl SynchronizedParameters {
    pub fn new(system_vars: SystemVars) -> Self {
        let synchronized = system_vars
            .iter_synced()
            .map(|v| v.name())
            .collect::<BTreeSet<_>>();
        Self {
            system_vars,
            synchronized,
            modified: BTreeSet::new(),
        }
    }

    pub fn is_synchronized(&self, name: &str) -> bool {
        self.synchronized.contains(name)
    }

    /// Return a clone of the set of names of synchronized values.
    ///
    /// Mostly useful when we need to iterate over each value, while still
    /// maintaining a mutable reference of the surrounding
    /// [SynchronizedParameters] instance.
    pub fn synchronized(&self) -> BTreeSet<&'static str> {
        self.synchronized.clone()
    }

    /// Return a vector of [ModifiedParameter] instances that need to be pushed
    /// to the backend and reset this set to the empty set for future calls.
    ///
    /// The set will start growing again as soon as we modify a parameter from
    /// the `synchronized` set with a [SynchronizedParameters::modify] call.
    pub fn modified(&mut self) -> Vec<ModifiedParameter> {
        let mut modified = BTreeSet::new();
        std::mem::swap(&mut self.modified, &mut modified);
        self.system_vars
            .iter_synced()
            .filter(move |var| modified.contains(var.name()))
            .map(|var| {
                let name = var.name().to_string();
                let value = var.value();
                let is_default = self.system_vars.is_default(&name, VarInput::Flat(&value)).expect("This will never panic because both the name and the value come from a `Var` instance");
                ModifiedParameter {
                    name,
                    value,
                    is_default,
                }
            })
            .collect()
    }

    /// Get the current in-memory value for the parameter identified by the
    /// given `name`.
    ///
    /// # Panics
    ///
    /// The method will panic if the name does not refer to a valid parameter.
    pub fn get(&self, name: &str) -> String {
        self.system_vars
            .get(name)
            .expect("valid system parameter name")
            .value()
    }

    /// Canonicalize a raw `value` for the parameter `name` to the same formatted
    /// form [`SynchronizedParameters::get`] returns, by parsing it through the
    /// system var and re-formatting.
    ///
    /// This lets values that are equal but differently encoded compare equal.
    /// For example LaunchDarkly serves a boolean as `"true"`, while the canonical
    /// formatting of a `bool` system var is `"on"`. Returns `None` if `name` is
    /// not a valid parameter or `value` does not parse for it.
    pub fn canonicalize(&self, name: &str, value: &str) -> Option<String> {
        self.system_vars
            .parse(name, VarInput::Flat(value))
            .ok()
            .map(|value| value.format())
    }

    /// Try to modify the in-memory entry for `name` in the SystemVars backing
    /// this [SynchronizedParameters] instance.
    ///
    /// This will call `SystemVars::reset` iff `value` is the default for this
    /// `name` and `SystemVars::set` otherwise.
    ///
    /// As a side effect, the modified set will be changed to contain `name` iff
    /// the in-memory entry for `name` was modified **and** `name` is in the
    /// `synchronized` set.
    ///
    /// Return `true` iff the backing in-memory value for this `name` has
    /// changed.
    pub fn modify(&mut self, name: &str, value: &str) -> bool {
        // It's OK to call `unwrap_or(false)` here because for fixed `name`
        // and `value` an error in `self.is_default(name, value)` implies
        // the same error in `self.system_vars.set(name, value)`.
        let value = VarInput::Flat(value);
        let modified = if self.system_vars.is_default(name, value).unwrap_or(false) {
            self.system_vars.reset(name)
        } else {
            self.system_vars.set(name, value)
        };

        match modified {
            Ok(true) => {
                // Track modified parameters from the "synchronized" set.
                if let Some(name) = self.synchronized.get(name) {
                    self.modified.insert(name);
                }
                true
            }
            Ok(false) => {
                // The value was the same as the current one.
                false
            }
            Err(e) => {
                tracing::error!("cannot modify system parameter {}: {}", name, e);
                false
            }
        }
    }

    pub fn enable_launchdarkly(&self) -> bool {
        let var_name = self.get(ENABLE_LAUNCHDARKLY.name());
        let var_input = VarInput::Flat(&var_name);
        bool::parse(var_input).expect("This is known to be a bool")
    }

    /// Whether scoped (per-cluster and per-replica) system parameters are
    /// evaluated. Read from this working copy so the sync loop can gate the
    /// scoped reconcile without taking a catalog snapshot.
    pub fn enable_scoped_system_parameters(&self) -> bool {
        let var_name = self.get(ENABLE_SCOPED_SYSTEM_PARAMETERS.name());
        let var_input = VarInput::Flat(&var_name);
        bool::parse(var_input).expect("This is known to be a bool")
    }
}

pub struct ModifiedParameter {
    pub name: String,
    pub value: String,
    pub is_default: bool,
}

#[cfg(test)]
mod tests {
    use mz_sql::session::vars::SystemVars;

    use super::SynchronizedParameters;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_github_18189() {
        let vars = SystemVars::default();
        let mut sync = SynchronizedParameters::new(vars);
        assert!(sync.modify("allowed_cluster_replica_sizes", "1,2"));
        assert_eq!(sync.get("allowed_cluster_replica_sizes"), r#""1", "2""#);
        assert!(sync.modify("allowed_cluster_replica_sizes", ""));
        assert_eq!(sync.get("allowed_cluster_replica_sizes"), "");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_canonicalize_bridges_bool_encodings() {
        let vars = SystemVars::default();
        let sync = SynchronizedParameters::new(vars);

        // A `bool` system var formats canonically as "on"/"off", while
        // LaunchDarkly serves booleans as "true"/"false". The scoped
        // differs-from-env test compares a raw LD value against `get()`, so
        // canonicalization must bridge the two spellings, otherwise every
        // boolean flag would register as differing, even on a FALLTHROUGH that
        // serves the env-wide value. (`enable_eager_delta_joins` is a scoped
        // `bool` feature flag, default off.)
        let name = "enable_eager_delta_joins";
        let off = sync.get(name);
        assert_eq!(off, "off");

        // The LD spellings canonicalize to the same form as the var's own.
        assert_eq!(sync.canonicalize(name, "false").as_deref(), Some("off"));
        assert_eq!(sync.canonicalize(name, "true").as_deref(), Some("on"));
        assert_eq!(
            sync.canonicalize(name, "off"),
            sync.canonicalize(name, "false")
        );
        assert_eq!(
            sync.canonicalize(name, "on"),
            sync.canonicalize(name, "true")
        );

        // The crux: a scoped "false" must match the env-wide "off" baseline, so
        // it is dropped rather than recorded as a spurious override.
        assert_eq!(
            sync.canonicalize(name, "false").as_deref(),
            Some(off.as_str())
        );

        // An unparseable value yields `None`. The scoped recording path relies
        // on this to *skip* such values rather than store them: a stored
        // unparseable bool would later panic the optimizer's decode on every
        // plan. See `SystemParameterFrontend::evaluate_scoped_overrides`.
        assert_eq!(sync.canonicalize(name, "not-a-bool"), None);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_vars_are_synced() {
        let vars = SystemVars::default();
        let sync = SynchronizedParameters::new(vars);

        assert!(!sync.synchronized().is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_rbac_not_ld_controllable() {
        // Remote configuration must not be able to disable access control or
        // its own kill switch. See `SystemVars::iter_synced`.
        let vars = SystemVars::default();
        let sync = SynchronizedParameters::new(vars);

        assert!(!sync.is_synchronized("enable_rbac_checks"));
        assert!(!sync.is_synchronized("enable_launchdarkly"));
    }
}
