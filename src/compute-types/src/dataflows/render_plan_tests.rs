// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use mz_repr::RelationDesc;

#[mz_ore::test]
fn enrichment_preserves_execution_settings() {
    let input = GlobalId::User(1);
    let output = GlobalId::User(2);
    let schedule = RefreshSchedule {
        everies: Vec::new(),
        ats: vec![20u64.into(), 30u64.into()],
    };
    let desc = RelationDesc::empty();
    let mut df = DataflowDescription::<LirRelationExpr>::new("enrichment".into());
    df.as_of = Some(Antichain::from_elem(10u64.into()));
    df.until = Antichain::from_elem(40u64.into());
    df.initial_storage_as_of = Some(Antichain::from_elem(5u64.into()));
    df.refresh_schedule = Some(schedule.clone());
    df.time_dependence = Some(TimeDependence::new(Some(schedule.clone()), Vec::new()));
    df.source_imports.insert(
        input,
        SourceImport {
            desc: SourceInstanceDesc {
                storage_metadata: (),
                arguments: SourceInstanceArguments { operators: None },
                typ: SqlRelationType::empty(),
            },
            monotonic: true,
            with_snapshot: false,
            upper: Antichain::from_elem(0u64.into()),
        },
    );
    df.sink_exports.insert(
        output,
        ComputeSinkDesc {
            from: input,
            from_desc: desc.clone(),
            connection: ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                value_desc: desc,
                storage_metadata: (),
            }),
            with_snapshot: false,
            up_to: Antichain::from_elem(35u64.into()),
            non_null_assertions: Vec::new(),
            refresh_schedule: Some(schedule),
        },
    );
    let original = df.clone();
    let converted = df
        .into_render_plan(
            |id| {
                assert_eq!(id, input);
                Ok::<_, ()>((id, Antichain::from_elem(25u64.into())))
            },
            |id| {
                assert_eq!(id, output);
                Ok(id)
            },
        )
        .expect("metadata is available");
    assert_eq!(converted.as_of, original.as_of);
    assert_eq!(converted.until, original.until);
    assert_eq!(
        converted.initial_storage_as_of,
        original.initial_storage_as_of
    );
    assert_eq!(converted.refresh_schedule, original.refresh_schedule);
    assert_eq!(converted.time_dependence, original.time_dependence);
    assert_eq!(converted.debug_name, original.debug_name);
    let import = &converted.source_imports[&input];
    assert_eq!(import.desc.storage_metadata, input);
    assert_eq!(import.upper, Antichain::from_elem(25u64.into()));
    assert!(import.monotonic);
    assert!(!import.with_snapshot);
    let sink = &converted.sink_exports[&output];
    assert_eq!(sink.from, input);
    assert_eq!(sink.from_desc, original.sink_exports[&output].from_desc);
    assert_eq!(sink.up_to, original.sink_exports[&output].up_to);
    assert_eq!(
        sink.refresh_schedule,
        original.sink_exports[&output].refresh_schedule
    );
    assert!(!sink.with_snapshot);
    let ComputeSinkConnection::MaterializedView(connection) = &sink.connection else {
        panic!("MV connection must be preserved");
    };
    assert_eq!(connection.storage_metadata, output);
}
