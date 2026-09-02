// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Round-trip property test for `mz_audit_events.details`.
//!
//! The `mz_audit_events` MV reads the proto-encoded `EventDetails` from
//! `mz_internal.mz_catalog_raw` and reshapes it through the SQL scalar
//! `parse_catalog_audit_log_details` so consumers see the same JSON that the
//! prior BuiltinTable populator produced (`audit_log::EventDetails::as_json`).
//!
//! This test verifies that reshape end-to-end: for any `audit_log::EventDetails`
//! value, encoding it via the durable-catalog proto and running the helper on
//! the resulting JSON must yield `as_json()`. It is the safety net that catches
//! divergences a hand-written unit test may miss, in particular invisible
//! field-name diffs (`rehydration_time_estimate` vs `hydration_time_estimate`)
//! and version-dependent `skip_serializing_if`. Adding a new `EventDetails`
//! variant with an unhandled divergence will fail this test.

use mz_audit_log::EventDetails;
use mz_catalog_protos::objects::audit_log_event_v1;
use mz_expr::func::{EagerUnaryFunc, ParseCatalogAuditLogDetails};
use mz_proto::RustType;
use mz_repr::adt::jsonb::Jsonb;
use proptest::prelude::*;

proptest! {
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // slow
    fn parse_catalog_audit_log_details_round_trip(evt in any::<EventDetails>()) {
        let expected = evt.as_json();

        // Simulate what `mz_catalog_raw` exposes at `e->'details'`: convert
        // the audit-log value to the proto used by the durable catalog, then
        // serialize the proto with the default externally-tagged serde shape.
        let proto: audit_log_event_v1::Details = evt.into_proto();
        let proto_json = serde_json::to_value(&proto).expect("proto serializes");
        let jsonb = Jsonb::from_serde_json(proto_json).expect("valid jsonb");

        let got = ParseCatalogAuditLogDetails
            .call(jsonb.as_ref())
            .expect("helper succeeded");
        let got_json = got.as_ref().to_serde_json();

        prop_assert_eq!(got_json, expected, "variant: {:?}", evt);
    }
}
