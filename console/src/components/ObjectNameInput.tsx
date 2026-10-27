// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { forwardRef, Input, InputProps } from "@chakra-ui/react";
import React from "react";

const ObjectNameInput = forwardRef<InputProps, "input">((props, ref) => {
  return (
    <Input ref={ref} autoCorrect="off" size="sm" data-1p-ignore {...props} />
  );
});

export default ObjectNameInput;
