// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as t from "@babel/types";
import { JSXAttribute } from "@babel/types";
import { Template } from "@svgr/babel-plugin-transform-svg-component";

function getJsxValue(attr: JSXAttribute) {
  if (attr.value && "expression" in attr.value) {
    return t.jsxExpressionContainer(attr.value.expression);
  }
  if (attr.value && "value" in attr.value) {
    return t.stringLiteral(attr.value.value);
  }
}

export const svgrTemplate: Template = (variables, context) => {
  const svgAttributes = variables.jsx.openingElement.attributes
    .map((attr) => {
      if ("name" in attr) {
        const value = getJsxValue(attr);
        if (!value) return;
        const name =
          typeof attr.name.name === "string"
            ? t.jsxIdentifier(attr.name.name)
            : attr.name.name;
        return t.jsxAttribute(name, value);
      }
    })
    // filter out undefined elements, which represent unsupported attribute types
    .filter((a) => a) as JSXAttribute[];
  // Chakra has a bug where setting a color in the theme prevents overriding it with
  // the color prop, this is a workaround. https://github.com/chakra-ui/chakra-ui/issues/7740
  const colorAttribute = t.jsxAttribute(
    t.jsxIdentifier("color"),
    t.stringLiteral("foreground.secondary"),
  );
  const fillAttribute = t.jsxAttribute(
    t.jsxIdentifier("fill"),
    t.stringLiteral("none"),
  );
  const refAttribute = t.jsxAttribute(
    t.jsxIdentifier("ref"),
    t.jsxExpressionContainer(t.identifier("ref")),
  );
  const svgOpeningTag = t.jsxOpeningElement(t.jsxIdentifier("Icon"), [
    refAttribute,
    colorAttribute,
    fillAttribute,
    ...svgAttributes,
    t.jsxSpreadAttribute(t.identifier("props")),
  ]);
  const svgClosingTag = t.jsxClosingElement(t.jsxIdentifier("Icon"));
  const svgElement = t.jsxElement(
    svgOpeningTag,
    svgClosingTag,
    variables.jsx.children,
  );
  return context.tpl`
    import { Icon, forwardRef, IconProps } from "@chakra-ui/react";
    ${variables.imports}
    ${variables.interfaces}

    export const ${variables.componentName} = forwardRef<IconProps, "svg">((${variables.props}, ref) => {
      return ${svgElement};
    });

    ${variables.exports};
  `;
};
