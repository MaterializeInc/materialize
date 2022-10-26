// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use sentry_tracing::EventFilter;

pub fn mz_sentry_event_filter(meta: &tracing::Metadata<'_>) -> EventFilter {
    // special cases
    if meta.target() == "librdkafka" {
        return EventFilter::Ignore;
    }

    // default case
    sentry_tracing::default_event_filter(meta)
}
