// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::{Scope, Stream};

use super::util::source;
use crate::dataflow::types::{Diff, Timestamp};
use crate::repr::Datum;

/// Construct a null source that produces no data and immediately drops its
/// capability.
pub fn null<G>(scope: &G, name: &str) -> Stream<G, (Vec<Datum>, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let (stream, _cap) = source(scope, name, move |_info| move |_cap, _output| ());
    // Intentionally drop `cap`, the only strong reference to this source's
    // capability to produce data. This will cause the source to shut down
    // immediately.
    stream
}
