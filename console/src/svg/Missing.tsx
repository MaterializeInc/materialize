// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

type Props = {
  bgColor: string;
  fillColor: string;
};

const Missing = (props: Props) => (
  <svg viewBox="0 0 73 73" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <g
        id="missing"
        fill={props.fillColor}
        fillRule="evenodd"
        clipRule="evenodd"
      >
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M36.8 11.6C22.8824 11.6 11.6 22.8824 11.6 36.8C11.6 50.7176 22.8824 62 36.8 62C50.7176 62 62 50.7176 62 36.8C62 22.8824 50.7176 11.6 36.8 11.6ZM6 36.8C6 19.7896 19.7896 6 36.8 6C53.8104 6 67.6 19.7896 67.6 36.8C67.6 53.8104 53.8104 67.6 36.8 67.6C19.7896 67.6 6 53.8104 6 36.8Z"
        />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M37.5228 25.6684C36.2193 25.4448 34.8788 25.6897 33.7386 26.3598C32.5984 27.0299 31.7322 28.0819 31.2934 29.3294C30.7802 30.7882 29.1816 31.5548 27.7229 31.0416C26.2641 30.5284 25.4975 28.9299 26.0107 27.4711C26.8884 24.976 28.6209 22.872 30.9012 21.5319C33.1815 20.1917 35.8626 19.7018 38.4695 20.149C41.0764 20.5961 43.4409 21.9515 45.1443 23.9749C46.8473 25.998 47.7796 28.5584 47.776 31.2028C47.7748 35.4878 44.5979 38.3176 42.3292 39.83C41.1094 40.6432 39.9096 41.2411 39.0257 41.6339C38.5798 41.8321 38.2043 41.9825 37.9337 42.0856C37.7982 42.1372 37.6884 42.1772 37.6084 42.2057L37.5111 42.2397L37.4802 42.2503L37.4693 42.254L37.465 42.2554C37.4641 42.2557 37.4615 42.2566 36.576 39.6003L37.4615 42.2566C35.9944 42.7456 34.4087 41.9527 33.9197 40.4857C33.431 39.0196 34.2225 37.4351 35.6877 36.9449L35.6841 36.9461C35.6844 36.946 35.6846 36.946 35.6877 36.9449L35.7323 36.9293C35.7754 36.9139 35.846 36.8883 35.9402 36.8525C36.129 36.7805 36.4097 36.6684 36.7513 36.5166C37.4425 36.2094 38.3426 35.7573 39.2229 35.1705C41.1537 33.8833 42.176 32.5138 42.176 31.2003L42.176 31.1961C42.178 29.8736 41.7119 28.5931 40.8602 27.5814C40.0085 26.5696 38.8262 25.8919 37.5228 25.6684Z"
        />
        <path
          fillRule="evenodd"
          clipRule="evenodd"
          d="M34 50.8C34 49.2536 35.2536 48 36.8 48H36.828C38.3744 48 39.628 49.2536 39.628 50.8C39.628 52.3464 38.3744 53.6 36.828 53.6H36.8C35.2536 53.6 34 52.3464 34 50.8Z"
        />
      </g>
      <mask id="outsideIconOnly">
        <rect x="0" y="0" width="100" height="100" fill="white" />
        <use xlinkHref="#missing" fill="black" />
      </mask>
    </defs>
    <use
      xlinkHref="#missing"
      strokeWidth="16px"
      stroke={props.bgColor}
      fill="none"
      mask="url(#outsideIconOnly)"
    />
    <use xlinkHref="#missing" />
  </svg>
);

export default Missing;
