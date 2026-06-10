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

import DropRoleModal from "./DropRoleModal";

export interface DropRoleMenuItemProps {
  roleName: string;
  ownedObjectsCount?: number;
  onSuccess?: () => void;
}

const DropRoleMenuItem = ({
  roleName,
  ownedObjectsCount = 0,
  onSuccess,
}: DropRoleMenuItemProps) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <>
      <MenuItem
        onClick={(e) => {
          e.stopPropagation();
          onOpen();
        }}
        textStyle="text-ui-med"
        color={colors.accent.red}
      >
        Drop role
      </MenuItem>
      {isOpen && (
        <DropRoleModal
          isOpen
          onClose={onClose}
          roleName={roleName}
          ownedObjectsCount={ownedObjectsCount}
          onSuccess={onSuccess}
        />
      )}
    </>
  );
};

export default DropRoleMenuItem;
