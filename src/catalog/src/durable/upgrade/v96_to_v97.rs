// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adds the optional `slot` field to `ManagedLocation`. Existing rows decode
//! with the field absent and re-encode unchanged, so nothing is rewritten.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::objects_v96 as v96;
use crate::durable::upgrade::objects_v97 as v97;

pub fn upgrade(
    _snapshot: Vec<v96::StateUpdateKind>,
) -> Vec<MigrationAction<v96::StateUpdateKind, v97::StateUpdateKind>> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn absent_slot_reencodes_unchanged() {
        let old = v96::ManagedLocation {
            size: "100cc".into(),
            availability_zones: vec!["az1".into()],
            internal: false,
            billed_as: None,
            pending: false,
        };
        let json = serde_json::to_value(&old).expect("serializable");
        let new: v97::ManagedLocation = serde_json::from_value(json.clone()).expect("compatible");
        assert_eq!(new.slot, None);
        assert_eq!(serde_json::to_value(&new).expect("serializable"), json);
    }
}
