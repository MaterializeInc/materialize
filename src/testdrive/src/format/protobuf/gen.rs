// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Module containing generated proto code

use protobuf::descriptor::FileDescriptorSet;
use protobuf::RepeatedField;

pub mod simple;
pub mod billing;

pub fn descriptors() -> FileDescriptorSet {
    let mut fds = FileDescriptorSet::new();
    fds.set_file(RepeatedField::from_vec(vec![
        simple::file_descriptor_proto().clone(),
        billing::file_descriptor_proto().clone(),
    ]));
    fds
}
