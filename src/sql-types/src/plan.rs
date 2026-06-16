// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leaf plan data types (cluster schedule, network policy rules).

use std::str::FromStr;
use std::time::Duration;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use crate::ParseError;

#[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterSchedule {
    /// The system won't automatically turn the cluster On or Off.
    Manual,
    /// The cluster will be On when a REFRESH materialized view on it needs to refresh.
    /// `hydration_time_estimate` determines how much time before a refresh to turn the
    /// cluster On, so that it can rehydrate already before the refresh time.
    Refresh { hydration_time_estimate: Duration },
}

impl Default for ClusterSchedule {
    fn default() -> Self {
        // (Has to be consistent with `impl Default for ClusterScheduleOptionValue`.)
        ClusterSchedule::Manual
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct NetworkPolicyRule {
    pub name: String,
    pub action: NetworkPolicyRuleAction,
    pub address: PolicyAddress,
    pub direction: NetworkPolicyRuleDirection,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash
)]
pub enum NetworkPolicyRuleAction {
    Allow,
}

impl std::fmt::Display for NetworkPolicyRuleAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allow => write!(f, "allow"),
        }
    }
}
impl TryFrom<&str> for NetworkPolicyRuleAction {
    type Error = ParseError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "ALLOW" => Ok(Self::Allow),
            _ => Err(ParseError::Unstructured(
                "Allow is the only valid option".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum NetworkPolicyRuleDirection {
    Ingress,
}
impl std::fmt::Display for NetworkPolicyRuleDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingress => write!(f, "ingress"),
        }
    }
}
impl TryFrom<&str> for NetworkPolicyRuleDirection {
    type Error = ParseError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "INGRESS" => Ok(Self::Ingress),
            _ => Err(ParseError::Unstructured(
                "Ingress is the only valid option".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct PolicyAddress(pub IpNet);
impl std::fmt::Display for PolicyAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string())
    }
}
impl From<String> for PolicyAddress {
    fn from(value: String) -> Self {
        Self(IpNet::from_str(&value).expect("expected value to be IpNet"))
    }
}
impl TryFrom<&str> for PolicyAddress {
    type Error = ParseError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let net = IpNet::from_str(value).map_err(|_| {
            ParseError::Unstructured("Value must be valid IPV4 or IPV6 CIDR".into())
        })?;
        Ok(Self(net))
    }
}

impl Serialize for PolicyAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}", &self.0))
    }
}
