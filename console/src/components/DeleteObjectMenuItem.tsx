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

import { DeletableObjectType } from "~/api/materialize/buildDropObjectStatement";
import { DatabaseObject } from "~/api/materialize/types";
import { MaterializeTheme } from "~/theme";

import DeleteObjectModal from "./DeleteObjectModal";

export interface DeleteObjectMenuItemProps {
  selectedObject: DatabaseObject;
  onSuccessAction: () => void;
  objectType: DeletableObjectType;
}

const DeleteObjectMenuItem = ({
  selectedObject,
  onSuccessAction: onSuccessAction,
  objectType,
}: DeleteObjectMenuItemProps) => {
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
        Drop {objectType.toLowerCase()}
      </MenuItem>
      {isOpen && (
        <DeleteObjectModal
          isOpen
          onClose={() => {
            onClose();
          }}
          onSuccess={onSuccessAction}
          dbObject={selectedObject}
          objectType={objectType}
        />
      )}
    </>
  );
};

export default DeleteObjectMenuItem;
