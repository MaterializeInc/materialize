// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::adt::mz_acl_item::MzAclItem;

sqlfunc!(
    #[sqlname = "mz_aclitem_grantor"]
    fn mz_acl_item_grantor(mz_acl_item: MzAclItem) -> String {
        mz_acl_item.grantor.to_string()
    }
);

sqlfunc!(
    #[sqlname = "mz_aclitem_grantee"]
    fn mz_acl_item_grantee(mz_acl_item: MzAclItem) -> String {
        mz_acl_item.grantee.to_string()
    }
);

sqlfunc!(
    #[sqlname = "mz_aclitem_privileges"]
    fn mz_acl_item_privileges(mz_acl_item: MzAclItem) -> String {
        mz_acl_item.acl_mode.to_string()
    }
);
