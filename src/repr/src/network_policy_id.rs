// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Re-export NetworkPolicyId from mz-catalog-types
pub use mz_catalog_types::NetworkPolicyId;

#[mz_ore::test]
fn test_network_policy_id_parsing() {
    let s = "s42";
    let network_policy_id: NetworkPolicyId = s.parse().unwrap();
    assert_eq!(NetworkPolicyId::System(42), network_policy_id);
    assert_eq!(s, network_policy_id.to_string());

    let s = "u666";
    let network_policy_id: NetworkPolicyId = s.parse().unwrap();
    assert_eq!(NetworkPolicyId::User(666), network_policy_id);
    assert_eq!(s, network_policy_id.to_string());

    let s = "d23";
    mz_ore::assert_err!(s.parse::<NetworkPolicyId>());

    let s = "asfje90uf23i";
    mz_ore::assert_err!(s.parse::<NetworkPolicyId>());

    let s = "";
    mz_ore::assert_err!(s.parse::<NetworkPolicyId>());
}
