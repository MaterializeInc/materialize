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
 * Link to the Schedule a Demo page on the marketing website.
 * Used as replacement for ChiliPiper meeting scheduling.
 */

import { LinkProps } from "@chakra-ui/react";
import React from "react";

import TextLink from "~/components/TextLink";

const SCHEDULE_DEMO_URL = "https://materialize.com/demo/";

const LINK_PROPS = {
  href: SCHEDULE_DEMO_URL,
  target: "_blank",
  rel: "noopener",
} as const;

const ScheduleDemoLink = (props: LinkProps) => {
  return <TextLink {...LINK_PROPS} {...props} />;
};

export default ScheduleDemoLink;
