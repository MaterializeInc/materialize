// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use repr::adt::system::{Oid, RegProc, RegType};

sqlfunc!(
    #[sqlname = "regproctooid"]
    #[preserves_uniqueness = true]
    fn cast_reg_proc_to_oid(a: RegProc) -> Oid {
        Oid(a.0)
    }
);

sqlfunc!(
    #[sqlname = "regtypetooid"]
    #[preserves_uniqueness = true]
    fn cast_reg_type_to_oid(a: RegType) -> Oid {
        Oid(a.0)
    }
);
