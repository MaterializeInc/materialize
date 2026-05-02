// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MenuItem, useDisclosure, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

import RemoveUserModal from "./RemoveUserModal";

export interface RemoveUserMenuItemProps {
  roleName: string;
  memberName: string;
  onSuccess?: () => void;
}

const RemoveUserMenuItem = ({
  roleName,
  memberName,
  onSuccess,
}: RemoveUserMenuItemProps) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <>
      <MenuItem
        onClick={(e) => {
          e.stopPropagation();
          onOpen();
        }}
        color={colors.accent.red}
      >
        Remove user
      </MenuItem>
      {isOpen && (
        <RemoveUserModal
          isOpen
          onClose={onClose}
          roleName={roleName}
          memberName={memberName}
          onSuccess={onSuccess}
        />
      )}
    </>
  );
};

export default RemoveUserMenuItem;
