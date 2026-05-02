// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, TextProps, useTheme } from "@chakra-ui/react";
import React from "react";

import ScheduleDemoLink from "~/components/ScheduleDemoLink";
import { MaterializeTheme } from "~/theme";

const ContactSalesCta = (props: TextProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Text textStyle="text-small" color={colors.foreground.secondary} {...props}>
      Have questions? <ScheduleDemoLink>Talk to our team</ScheduleDemoLink>
    </Text>
  );
};

export default ContactSalesCta;
