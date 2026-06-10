// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SystemStyleObjectRecord } from "@chakra-ui/react";

import { lineHeightFromFontSize, trackingFromFontSize } from "./utils";

interface TextStyle {
  fontFamily: string;
  fontSize: string;
  lineHeight: string;
  fontWeight: string;
  letterSpacing: string;
  fontFeatureSettings?: string;
}

export interface TextStyles extends SystemStyleObjectRecord {
  "heading-xxl": TextStyle;
  "heading-xl": TextStyle;
  "heading-lg": TextStyle;
  "heading-md": TextStyle;
  "heading-sm": TextStyle;
  "heading-xs": TextStyle;
  "text-base": TextStyle;
  "text-small": TextStyle;
  "text-ui-med": TextStyle;
  "text-ui-reg": TextStyle;
  monospace: TextStyle;
}

export const typographySystem: TextStyles = {
  "heading-xxl": {
    fontFamily: "Inter",
    fontSize: "40px",
    lineHeight: lineHeightFromFontSize(40),
    fontWeight: "600",
    letterSpacing: trackingFromFontSize(40),
  },
  "heading-xl": {
    fontFamily: "Inter",
    fontSize: "32px",
    lineHeight: lineHeightFromFontSize(32),
    fontWeight: "600",
    letterSpacing: trackingFromFontSize(32),
  },
  "heading-lg": {
    fontFamily: "Inter",
    fontSize: "24px",
    lineHeight: lineHeightFromFontSize(24),
    fontWeight: "600",
    letterSpacing: trackingFromFontSize(24),
  },
  "heading-md": {
    fontFamily: "Inter",
    fontSize: "20px",
    lineHeight: lineHeightFromFontSize(20),
    fontWeight: "500",
    letterSpacing: trackingFromFontSize(20),
  },
  "heading-sm": {
    fontFamily: "Inter",
    fontSize: "18px",
    lineHeight: lineHeightFromFontSize(18),
    fontWeight: "500",
    letterSpacing: trackingFromFontSize(18),
  },
  "heading-xs": {
    fontFamily: "Inter",
    fontSize: "16px",
    lineHeight: lineHeightFromFontSize(16),
    fontWeight: "500",
    letterSpacing: trackingFromFontSize(16),
  },
  "text-base": {
    fontFamily: "Inter",
    fontSize: "14px",
    lineHeight: lineHeightFromFontSize(14),
    fontWeight: "400",
    letterSpacing: trackingFromFontSize(14),
    fontFeatureSettings: "'tnum' on, 'lnum' on, 'cv06' on, 'cv10' on",
  },
  "text-small": {
    fontFamily: "Inter",
    fontSize: "12px",
    lineHeight: lineHeightFromFontSize(12),
    fontWeight: "400",
    letterSpacing: trackingFromFontSize(12),
    fontFeatureSettings: "'tnum' on, 'lnum' on, 'cv06' on, 'cv10' on",
  },
  "text-small-heavy": {
    fontFamily: "Inter",
    fontSize: "12px",
    lineHeight: lineHeightFromFontSize(12),
    fontWeight: "500",
    letterSpacing: trackingFromFontSize(12),
    fontFeatureSettings: "'tnum' on, 'lnum' on, 'cv06' on, 'cv10' on",
  },
  "text-ui-med": {
    fontFamily: "Inter",
    fontSize: "14px",
    lineHeight: lineHeightFromFontSize(14),
    fontWeight: "500",
    letterSpacing: trackingFromFontSize(14),
    fontFeatureSettings: "'tnum' on, 'lnum' on, 'cv06' on, 'cv10' on",
  },
  "text-ui-reg": {
    fontFamily: "Inter",
    fontSize: "14px",
    lineHeight: lineHeightFromFontSize(14),
    fontWeight: "400",
    letterSpacing: trackingFromFontSize(14),
    fontFeatureSettings: "'tnum' on, 'lnum' on, 'cv06' on, 'cv10' on",
  },
  monospace: {
    fontFamily: "Roboto Mono",
    fontSize: "14px",
    lineHeight: lineHeightFromFontSize(14),
    fontWeight: "400",
    letterSpacing: "auto",
  },
};
