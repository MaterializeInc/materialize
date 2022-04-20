// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Types defined in relation and scalar are both defined in
// relation_and_scalar.proto. Thus we include them here, such that relation.rs
// and scalar.rs can selectively pub export from this private module.
include!(concat!(env!("OUT_DIR"), "/mz_repr.relation_and_scalar.rs"));
