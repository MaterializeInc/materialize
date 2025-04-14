// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Custom Protobuf types for the [`url`] crate.

use proptest::prelude::Strategy;
use url::Url;

pub fn any_url() -> impl Strategy<Value = Url> {
    r"(http|https)://[a-z][a-z0-9]{0,10}/?([a-z0-9]{0,5}/?){0,3}".prop_map(|s| s.parse().unwrap())
}
