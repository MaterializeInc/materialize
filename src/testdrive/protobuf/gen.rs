// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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
