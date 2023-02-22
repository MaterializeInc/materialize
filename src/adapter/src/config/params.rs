// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_sql::ast::{
    AlterSystemResetStatement, AlterSystemSetStatement, Ident, Raw, SetVariableTo,
    SetVariableValue, Statement, Value,
};

use crate::session::vars::{parse_set_variable_value, SystemVars};

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
    /// to the backend and reset this set to the empty set.
    pub fn modified(&mut self) -> Vec<ModifiedParameter> {
        let mut modified = BTreeSet::new();
        std::mem::swap(&mut self.modified, &mut modified);
        self.system_vars
            .iter_synced()
            .filter(move |var| modified.contains(var.name()))
            .map(|var| {
                let name = var.name().to_string();
                let value = var.value();
                let values = parse_set_variable_value(&value).expect("This will never panic because both the name and the value come from a `Var` instance");
                let is_default = self.system_vars.is_default(&name, &values).expect("This will never panic because both the name and the value come from a `Var` instance");
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

    /// Try to modify the in-memory `value` for `name` backing this
    /// [SynchronizedParameters] instace, calling `SystemVars::reset` iff
    /// `value` is the default for this `name` and `SystemVars::set` otherwise.
    ///
    /// Return `true` iff the backing in-memory value for this `name` has
    /// changed.
    pub fn modify(&mut self, name: &str, value: &str) -> bool {
        // Resolve name to &'static str and assert that the system parameter can
        // be indeed modified.
        if let Some(name) = self.synchronized.get(name) {
            // It's OK to call `unwrap_or(false)` here because for fixed `name`
            // and `value` an error in `self.is_default(name, value)` implies
            // the same error in `self.system_vars.set(name, value)`.

            let modified = parse_set_variable_value(value).and_then(|values| {
                if self.system_vars.is_default(name, &values).unwrap_or(false) {
                    self.system_vars.reset(name)
                } else {
                    self.system_vars.set(name, &values)
                }
            });
            match modified {
                Ok(true) => {
                    self.modified.insert(name);
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
        } else {
            tracing::error!("cannot modify unsynchronized system parameter {}", name);
            false
        }
    }
}

pub struct ModifiedParameter {
    pub name: String,
    pub value: String,
    pub is_default: bool,
}

impl ModifiedParameter {
    pub fn as_alter_system(&self) -> String {
        match self.is_default {
            true => format!("ALTER SYSTEM RESET {}", self.name),
            false => format!("ALTER SYSTEM SET {} = {}", self.name, self.value),
        }
    }

    pub fn as_stmt(&self) -> Statement<Raw> {
        match self.is_default {
            true => Statement::AlterSystemReset(AlterSystemResetStatement {
                name: Ident::from(self.name.clone()),
            }),
            false => Statement::AlterSystemSet(AlterSystemSetStatement {
                name: Ident::from(self.name.clone()),
                to: SetVariableTo::Values(vec![SetVariableValue::Literal(Value::String(
                    self.value.clone(),
                ))]),
            }),
        }
    }
}
