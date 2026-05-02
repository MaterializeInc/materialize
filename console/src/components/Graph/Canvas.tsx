// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Center,
  Divider,
  Flex,
  HStack,
  IconButton,
  Text,
  useTheme,
} from "@chakra-ui/react";
import dagre from "@dagrejs/dagre";
import React from "react";

import { useCamera } from "~/hooks/useCamera";
import { useDrag } from "~/hooks/useDrag";
import FrameSelectionIcon from "~/svg/FrameSelectionIcon";
import ZoomInIcon from "~/svg/ZoomInIcon";
import ZoomOutIcon from "~/svg/ZoomOutIcon";
import { MaterializeTheme } from "~/theme";

export interface CanvasProps {
  width: number | undefined;
  height: number | undefined;
  selectedNode: dagre.Node | null;
  /** Right offset for the canvas and controls (e.g., sidebar width). Defaults to 0. */
  rightOffset?: number;
  /** Z-index for the zoom controls. Defaults to 1. */
  controlsZIndex?: number;
}

/**
 * Margin around the graph
 *
 * We use this in two ways:
 * 1) to set the minimum absolute top and left values
 * 2) added to the height and width of the graph element
 */
export const GRAPH_MARGIN = 40;

export const Canvas = ({
  height,
  width,
  selectedNode,
  rightOffset = 0,
  controlsZIndex = 1,
  children,
}: React.PropsWithChildren<CanvasProps>) => {
  const {
    camera,
    cameraRef,
    formatZoomValue,
    panBy,
    panTo,
    resetZoom,
    cssTransform,
    zoomIn,
    zoomOut,
  } = useCamera();

  useDrag({
    ref: cameraRef,
    onStart: () => {
      cameraRef.current?.style.setProperty("cursor", "grabbing");
    },
    onStop: () => {
      cameraRef.current?.style.removeProperty("cursor");
    },
    onDrag: (_, { pointDelta }) => {
      if (!pointDelta) return;
      panBy(-pointDelta.x, -pointDelta.y);
    },
  });

  const { height: viewportHeight, width: viewportWidth } = camera;
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const panToSelectedNode = React.useCallback(() => {
    if (!selectedNode || !viewportWidth || !viewportHeight || !height || !width)
      return;

    // Dagre node x and y are center points
    const { y: nodeY, x: nodeX } = selectedNode;

    const yOffset = nodeY - height / 2;
    const xOffset = nodeX - width / 2;

    panTo(-xOffset, -yOffset);
  }, [height, panTo, selectedNode, viewportHeight, viewportWidth, width]);

  React.useEffect(() => {
    panToSelectedNode();
  }, [panToSelectedNode]);

  return (
    <>
      <HStack
        zIndex={controlsZIndex}
        position="absolute"
        bottom={4}
        right={rightOffset + 16}
        spacing="0"
        background={colors.background.primary}
        border="1px solid"
        borderColor={colors.border.primary}
        borderRadius="md"
        shadow={shadows.level1}
        boxSizing="border-box"
        width="fit-content"
        overflow="hidden"
      >
        <IconButton
          variant="inline"
          aria-label="Reset zoom"
          icon={<FrameSelectionIcon />}
          onClick={() => {
            panToSelectedNode();
            resetZoom();
          }}
        />
        <IconButton
          variant="inline"
          aria-label="Zoom in"
          icon={<ZoomInIcon />}
          onClick={() => {
            zoomIn();
          }}
        />
        <IconButton
          variant="inline"
          aria-label="Zoom out"
          icon={<ZoomOutIcon />}
          onClick={() => {
            zoomOut();
          }}
        />
        <Center w={6}>
          <Divider
            orientation="vertical"
            color={colors.border.secondary}
            height="24px"
          />
        </Center>
        <Text
          textStyle="text-ui-med"
          color={colors.foreground.primary}
          pl="1"
          pr="5"
          minW="16"
          textAlign="center"
        >
          {formatZoomValue(camera.z)}
        </Text>
      </HStack>
      <Flex
        ref={cameraRef as React.MutableRefObject<HTMLDivElement>}
        top="0"
        left="0"
        right={rightOffset}
        bottom="0"
        display="grid"
        placeItems="center"
        position="absolute"
        overflow="hidden"
        scrollBehavior="smooth"
        background={`radial-gradient(${colors.border.secondary} 1px, ${colors.background.primary} 1px) 0px 0px / 24px 24px`}
        _hover={{ cursor: "grab" }}
      >
        {height && width ? (
          <Flex
            style={{
              // We explicitly set this as a style property otherwise everytime this property changes,
              // a new stylesheet is created
              transform: cssTransform,
            }}
            position="absolute"
            width={width}
            height={height}
          >
            {children}
          </Flex>
        ) : null}
      </Flex>
    </>
  );
};
