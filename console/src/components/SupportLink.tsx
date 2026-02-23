// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Call to action (CTA) components.
 */

import { Button, ButtonProps, LinkProps } from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";
import docUrls from "~/mz-doc-urls.json";

const LINK_PROPS = {
  href: docUrls["/docs/support/"],
  target: "_blank",
  rel: "noopener",
} as const;

const SupportLink = (props: LinkProps) => {
  return <TextLink {...LINK_PROPS} {...props} />;
};

export const SupportButton = ({
  children,
  ...buttonProps
}: React.PropsWithChildren<ButtonProps>) => {
  return (
    <Button variant="outline" size="sm" as="a" {...LINK_PROPS} {...buttonProps}>
      {children}
    </Button>
  );
};

export default SupportLink;
