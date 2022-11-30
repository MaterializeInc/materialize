// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;

use crate::session::vars::Value;

/// A struct that defines the system parameters that should be synchronized
pub struct SynchronizedParameters {
    pub(crate) window_functions: SynchronizedParameter<bool>,
    pub(crate) allowed_cluster_replica_sizes: SynchronizedParameter<Vec<String>>,
    pub(crate) max_result_size: SynchronizedParameter<u32>,
}

impl Default for SynchronizedParameters {
    // TODO (15956): We probably should move the const ServerVar<T> declarations
    // from adapter::session::vars to this crate so we can bind the
    // `default_value` and `name` fields from those rather than copying them.
    fn default() -> Self {
        Self::new(true, Vec::<String>::new(), 1_073_741_824_u32)
    }
}

impl SynchronizedParameters {
    // TODO (15956): We probably should move the const ServerVar<T> declarations
    // from adapter::session::vars to this crate so we can bind the
    // `default_value` and `name` fields from those rather than copying them.
    pub fn new(
        window_functions: bool,
        allowed_cluster_replica_sizes: Vec<String>,
        max_result_size: u32,
    ) -> Self {
        Self {
            window_functions: SynchronizedParameter {
                name: "window_functions",
                default_value: true,
                current_value: window_functions,
                modified_flag: false,
            },
            allowed_cluster_replica_sizes: SynchronizedParameter {
                name: "allowed_cluster_replica_sizes",
                default_value: Vec::<String>::new(),
                current_value: allowed_cluster_replica_sizes,
                modified_flag: false,
            },
            max_result_size: SynchronizedParameter {
                name: "max_result_size",
                default_value: 1_073_741_824_u32,
                current_value: max_result_size,
                modified_flag: false,
            },
        }
    }

    pub fn iter_modified(self) -> impl Iterator<Item = (String, String)> {
        let mut modified = vec![];

        if self.window_functions.modified_flag {
            modified.push((
                self.window_functions.name.to_owned(),
                self.window_functions.current_value.format(),
            ));
        }
        if self.allowed_cluster_replica_sizes.modified_flag {
            modified.push((
                self.allowed_cluster_replica_sizes.name.to_owned(),
                self.allowed_cluster_replica_sizes.current_value.format(),
            ));
        }
        if self.max_result_size.modified_flag {
            modified.push((
                self.max_result_size.name.to_owned(),
                self.max_result_size.current_value.format(),
            ));
        }

        modified.into_iter()
    }
}

pub(crate) struct SynchronizedParameter<V: Value + 'static> {
    pub(crate) name: &'static str,
    pub(crate) default_value: V::Owned,
    pub(crate) current_value: V::Owned,
    pub(crate) modified_flag: bool,
}

impl<V: Value> SynchronizedParameter<V>
where
    V::Owned: Borrow<V> + PartialEq + Eq,
{
    /// Modify this parameter if the value can be parsed and is different than
    /// the `current_value`, logging an error if the parsing fails.
    pub(crate) fn modify(&mut self, value: &str) {
        match V::parse(value) {
            Ok(value) => {
                if self.current_value != value {
                    self.current_value = value;
                    self.modified_flag = true;
                }
            }
            Err(_error) => {
                // Errors (including parsing errors) are reported and recovered
                // from by not updating the parameter value.
                tracing::error!("Cannot parse value '{}' for parameter {}", value, self.name);
            }
        }
    }

    /// Return a derived [PushParameterRequest] iff `self` has been marked as
    /// modified.
    pub(crate) fn as_request<'a>(&'a mut self) -> Option<PushParameterRequest<'a>> {
        if self.modified_flag {
            Some(PushParameterRequest {
                parameter_flag: &mut self.modified_flag,
                parameter_name: self.name,
                parameter_value: if self.current_value != self.default_value {
                    Some(self.current_value.borrow().format())
                } else {
                    None
                },
            })
        } else {
            None
        }
    }
}

pub(crate) struct PushParameterRequest<'a> {
    pub(crate) parameter_flag: &'a mut bool,
    pub(crate) parameter_name: &'static str,
    pub(crate) parameter_value: Option<String>,
}

impl<'a> PushParameterRequest<'a> {
    pub(crate) fn sql_statement(&self) -> String {
        match &self.parameter_value {
            Some(value) => format!("ALTER SYSTEM SET {} = {}", self.parameter_name, value),
            None => format!("ALTER SYSTEM RESET {}", self.parameter_name),
        }
    }
}
