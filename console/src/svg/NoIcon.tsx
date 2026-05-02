// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Icon, IconProps } from "@chakra-ui/react";
import React from "react";

export interface NoIconProps extends IconProps {
  bgColor: string;
  fillColor: string;
}

const NoIcon = ({ fillColor, bgColor, ...iconProps }: NoIconProps) => (
  <Icon
    height="6"
    width="6"
    viewBox="0 0 73 73"
    xmlns="http://www.w3.org/2000/svg"
    {...iconProps}
  >
    <defs>
      <g id="slash" fill={fillColor} fillRule="evenodd" clipRule="evenodd">
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M36.0005 11.4998C22.4695 11.4998 11.5005 22.4688 11.5005 35.9998C11.5005 49.5307 22.4695 60.4998 36.0005 60.4998C49.5315 60.4998 60.5005 49.5307 60.5005 35.9998C60.5005 22.4688 49.5315 11.4998 36.0005 11.4998ZM5.50049 35.9998C5.50049 19.1551 19.1558 5.49976 36.0005 5.49976C52.8452 5.49976 66.5005 19.1551 66.5005 35.9998C66.5005 52.8444 52.8452 66.4998 36.0005 66.4998C19.1558 66.4998 5.50049 52.8444 5.50049 35.9998Z"
        />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M14.4369 14.4362C15.6085 13.2646 17.508 13.2646 18.6795 14.4362L57.5645 53.3212C58.7361 54.4927 58.7361 56.3922 57.5645 57.5638C56.393 58.7354 54.4935 58.7354 53.3219 57.5638L14.4369 18.6788C13.2653 17.5072 13.2653 15.6077 14.4369 14.4362Z"
        />
      </g>
      <mask id="outsideSlashOnly">
        <rect x="0" y="0" width="100" height="100" fill="white" />
        <use xlinkHref="#slash" fill="black" />
      </mask>
    </defs>
    <use
      xlinkHref="#slash"
      strokeWidth="14px"
      stroke={bgColor}
      fill="none"
      mask="url(#outsideSlashOnly)"
    />
    <use xlinkHref="#slash" />
  </Icon>
);

export default NoIcon;
