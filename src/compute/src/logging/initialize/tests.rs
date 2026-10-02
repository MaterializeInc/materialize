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
use crate::sharing::{ArrangementSharingRegistry, Publisher};
use crate::typedefs::{ErrBatcher, ErrBuilder, ErrSpine, RowRowSpine};

use super::publish_logging_index;

/// A runtime publishes its logging indexes only through a publishing [`Publisher`], and the
/// publication lasts as long as the token it returns.
#[mz_ore::test]
fn logging_indexes_publish_only_through_a_publishing_publisher() {
    for publishes in [true, false] {
        let id = GlobalId::System(1);
        let registry = ArrangementSharingRegistry::new();
        let publisher = if publishes {
            Publisher::Registry(registry.clone())
        } else {
            Publisher::None
        };

        let token = timely::execute_directly(move |worker| {
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

                let token =
                    publish_logging_index(&publisher, &scope.clone(), id, &oks.trace, &errs.trace);

                oks_input.advance_to(Timestamp::from(1_u64));
                oks_input.flush();
                errs_input.advance_to(Timestamp::from(1_u64));
                errs_input.flush();
                token
            })
        });

        assert_eq!(token.is_some(), publishes);
        assert_eq!(registry.handles(&id).is_some(), publishes);
        drop(token);
        assert!(registry.handles(&id).is_none());
    }
}
