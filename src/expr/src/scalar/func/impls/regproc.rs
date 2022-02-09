// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::adt::system::{Oid, RegClass, RegProc, RegType};

sqlfunc!(
    #[sqlname = "regclasstooid"]
    #[preserves_uniqueness = true]
    fn cast_reg_class_to_oid(a: RegClass) -> Oid {
        Oid(a.0)
    }
);

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
