// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::id_gen::{IdAllocatorInnerBitSet, IdHandle};
use uuid::Uuid;

/// Inner type of a [`ConnectionId`], `u32` for postgres compatibility.
///
/// Note: Generally you should not use this type directly, and instead use [`ConnectionId`].
pub type ConnectionIdType = u32;

/// An abstraction allowing us to name different connections.
pub type ConnectionId = IdHandle<ConnectionIdType, IdAllocatorInnerBitSet>;

/// Number of bits the org id is offset into a connection id.
pub const ORG_ID_OFFSET: usize = 19;

/// Extracts the lower 12 bits from an org id. These are later used as the [31, 20] bits of a
/// connection id to help route cancellation requests.
pub fn org_id_conn_bits(uuid: &Uuid) -> ConnectionIdType {
    let lower = uuid.as_u128();
    let lower = (lower & 0xFFF) << ORG_ID_OFFSET;
    let lower: u32 = lower.try_into().expect("must fit");
    lower
}

/// Returns the portion of the org's UUID present in connection id.
pub fn conn_id_org_uuid(conn_id: u32) -> String {
    const UPPER: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
    ];

    // Extract UUID from conn_id: upper 12 bits excluding the first.
    let orgid = usize::try_from((conn_id >> ORG_ID_OFFSET) & 0xFFF).expect("must cast");
    // Convert the bits into a 3 char string and inject into the resolver template.
    let mut dst = String::with_capacity(3);
    dst.push(UPPER[(orgid >> 8) & 0xf]);
    dst.push(UPPER[(orgid >> 4) & 0xf]);
    dst.push(UPPER[orgid & 0xf]);
    dst
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::connection::{conn_id_org_uuid, org_id_conn_bits};

    #[mz_ore::test]
    fn test_conn_org() {
        let uuid = Uuid::parse_str("9e37ec59-56f4-450a-acbd-18ff14f10ca8").unwrap();
        let lower = org_id_conn_bits(&uuid);
        let org_lower_uuid = conn_id_org_uuid(lower);
        assert_eq!(org_lower_uuid, "CA8");
    }
}
