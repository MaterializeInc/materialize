// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// System support functions that need to be written in C.

// When LLVM source-code based code coverage is active, it provides a strong
// version of this symbol that actually writes the code coverage information
// to disk.
__attribute__((weak))
int __llvm_profile_write_file() {
    return 0;
}
