// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  // This is the wrapper, the only place we should be importing it
  // eslint-disable-next-line no-restricted-imports
  Modal as ChakraModal,
  ModalProps,
  useBreakpointValue,
} from "@chakra-ui/react";
import React from "react";

/**
 * Wraps Chakra's modal and automatically disables focus lock for mobile viewports.
 */
export const Modal = (props: ModalProps) => {
  const blockScrollOnMount = useBreakpointValue(
    {
      base: false,
      lg: true,
    },
    { ssr: false },
  );
  return <ChakraModal blockScrollOnMount={blockScrollOnMount} {...props} />;
};
