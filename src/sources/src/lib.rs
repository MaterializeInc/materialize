// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::{Diff, Row, Timestamp};
use timely::dataflow::operators::to_stream::Event as TimelyEvent;

mod interface;
mod macros;
mod sources;

pub use interface::*;
use sources::*;

sources! {
    TickTock,
    TickTockSimple
}

pub type Error = ();
pub type Event = TimelyEvent<Option<Timestamp>, Result<(Row, Timestamp, Diff), Error>>;
