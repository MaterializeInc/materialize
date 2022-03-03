// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_smithy_client::erase::DynConnector;
use aws_smithy_client::hyper_ext::Adapter;

pub fn connector() -> DynConnector {
    DynConnector::new(Adapter::builder().build(mz_http_proxy::hyper::connector()))
}
