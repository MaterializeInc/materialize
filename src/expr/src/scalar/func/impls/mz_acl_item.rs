// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use crate::EvalError;
use mz_ore::str::StrExt;
use mz_repr::adt::mz_acl_item::{AclItem, AclMode, MzAclItem};
use mz_repr::adt::system::Oid;
use mz_repr::ArrayRustType;

sqlfunc!(
    #[sqlname = "mz_aclitem_grantor"]
    fn mz_acl_item_grantor(mz_acl_item: MzAclItem) -> String {
        mz_acl_item.grantor.to_string()
    }
);

sqlfunc!(
    #[sqlname = "aclitem_grantor"]
    fn acl_item_grantor(acl_item: AclItem) -> Oid {
        acl_item.grantor
    }
);

sqlfunc!(
    #[sqlname = "mz_aclitem_grantee"]
    fn mz_acl_item_grantee(mz_acl_item: MzAclItem) -> String {
        mz_acl_item.grantee.to_string()
    }
);

sqlfunc!(
    #[sqlname = "aclitem_grantee"]
    fn acl_item_grantee(acl_item: AclItem) -> Oid {
        acl_item.grantee
    }
);

sqlfunc!(
    #[sqlname = "mz_aclitem_privileges"]
    fn mz_acl_item_privileges(mz_acl_item: MzAclItem) -> String {
        mz_acl_item.acl_mode.to_string()
    }
);

sqlfunc!(
    #[sqlname = "aclitem_privileges"]
    fn acl_item_privileges(acl_item: AclItem) -> String {
        acl_item.acl_mode.to_string()
    }
);

sqlfunc!(
    #[sqlname = "mz_format_privileges"]
    fn mz_format_privileges(privileges: String) -> Result<ArrayRustType<String>, EvalError> {
        AclMode::from_str(&privileges)
            .map(|acl_mode| {
                ArrayRustType(
                    acl_mode
                        .explode()
                        .into_iter()
                        .map(|privilege| privilege.to_string())
                        .collect(),
                )
            })
            .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))
    }
);

sqlfunc!(
    #[sqlname = "mz_validate_privileges"]
    fn mz_validate_privileges(privileges: String) -> Result<bool, EvalError> {
        AclMode::parse_multiple_privileges(&privileges)
            .map(|_| true)
            .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))
    }
);

sqlfunc!(
    #[sqlname = "mz_validate_role_privilege"]
    fn mz_validate_role_privilege(privilege: String) -> Result<bool, EvalError> {
        let privilege_upper = privilege.to_uppercase();
        if privilege_upper != "MEMBER" && privilege_upper != "USAGE" {
            Err(EvalError::InvalidPrivileges(
                format!("{}", privilege.quoted()).into(),
            ))
        } else {
            Ok(true)
        }
    }
);
