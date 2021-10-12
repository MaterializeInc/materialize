// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal AWS utility library for Materialize.

#![warn(missing_docs, missing_debug_implementations)]
#![cfg_attr(nightly_doc_features, feature(doc_cfg))]

mod util;

pub mod config;

#[cfg_attr(nightly_doc_features, doc(cfg(feature = "kinesis")))]
#[cfg(feature = "kinesis")]
pub mod kinesis;

#[cfg_attr(nightly_doc_features, doc(cfg(feature = "s3")))]
#[cfg(feature = "s3")]
pub mod s3;

#[cfg_attr(nightly_doc_features, doc(cfg(feature = "sqs")))]
#[cfg(feature = "sqs")]
pub mod sqs;

#[cfg_attr(nightly_doc_features, doc(cfg(feature = "sts")))]
#[cfg(feature = "sts")]
pub mod sts;
