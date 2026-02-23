// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DeleteIcon } from "@chakra-ui/icons";
import { ButtonProps, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { ApiToken } from "~/api/frontegg/types";
import DangerActionModal from "~/components/DangerActionModal";
import { useDeleteApiToken } from "~/queries/frontegg";
import { MaterializeTheme } from "~/theme";

interface Props extends ButtonProps {
  token: ApiToken;
}

const DeleteAppPasswordModal = (props: Props) => {
  const { mutateAsync: deleteApiToken } = useDeleteApiToken();

  const { colors } = useTheme<MaterializeTheme>();

  return (
    <DangerActionModal
      title="Delete app password"
      aria-label="Delete app password"
      colorScheme="red"
      confirmIcon={<DeleteIcon />}
      actionText=""
      finalActionText="Delete"
      isDisabled={props.isDisabled}
      confirmText={props.token.description}
      onConfirm={async () => {
        await deleteApiToken({ token: props.token });
      }}
      size="sm"
      variant="outline"
    >
      <Text fontSize="sm" color={colors.foreground.primary}>
        Deleting this app password will revoke access to any devices or services
        using it to connect to Materialize.
      </Text>
    </DangerActionModal>
  );
};

export default DeleteAppPasswordModal;
