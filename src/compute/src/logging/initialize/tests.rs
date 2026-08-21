// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::input::Input;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_row_spine::{RowRowBatcher, RowRowBuilder};
use mz_timely_util::columnation::ColumnationChunker;

use crate::extensions::arrange::{KeyCollection, MzArrange};
use crate::render::errors::DataflowErrorSer;
use crate::server::ComputeRuntimeRole;
use crate::sharing::ArrangementSharingRegistry;
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, RowRowSpine};

use super::publish_logging_index;

/// A logging/introspection index is a `RowRow` `oks` arrangement plus an (empty) `errs`
/// arrangement, published into the sharing registry only by the maintenance runtime. Interactive
/// and Solo must not publish: interactive reads maintenance's slot rather than clobbering it with
/// its own empty copy, and Solo has no registry peer.
///
/// Builds real `RowRow`/`Err` arrangements (the exact types the logging path produces) and drives
/// [`publish_logging_index`] for each role, asserting only maintenance ends up published.
#[mz_ore::test]
fn maintenance_publishes_logging_index_others_do_not() {
    for (role, expect_published) in [
        (ComputeRuntimeRole::Maintenance, true),
        (ComputeRuntimeRole::Interactive, false),
        (ComputeRuntimeRole::Solo, false),
    ] {
        let id = GlobalId::System(1);
        let registry = ArrangementSharingRegistry::new();
        let registry_in = registry.clone();

        timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut oks_input, oks_collection) = scope.new_collection::<(Row, Row), Diff>();
                let oks = oks_collection.mz_arrange::<
                    ColumnationChunker<_>,
                    RowRowBatcher<_, _>,
                    RowRowBuilder<_, _>,
                    RowRowSpine<_, _>,
                >("test log oks");

                let (mut errs_input, errs_collection) =
                    scope.new_collection::<DataflowErrorSer, Diff>();
                let errs = KeyCollection::from(errs_collection).mz_arrange::<
                    ColumnationChunker<_>,
                    ErrBatcher<_, _>,
                    ErrBuilder<_, _>,
                    ErrSpine<_, _>,
                >("test log errs");

                publish_logging_index(
                    role,
                    &registry_in,
                    &scope.clone(),
                    id,
                    &oks.trace,
                    &errs.trace,
                );

                oks_input.advance_to(Timestamp::from(1_u64));
                oks_input.flush();
                errs_input.advance_to(Timestamp::from(1_u64));
                errs_input.flush();
            });
        });

        assert_eq!(
            registry.handles(&id, 0).is_some(),
            expect_published,
            "role {role:?} publication mismatch"
        );
    }
}
