// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Center, Spinner } from "@chakra-ui/react";
import React from "react";
import { useNavigate, useOutletContext, useParams } from "react-router-dom";

import { SideDrawer } from "~/components/SideDrawer";

import { MaintainedObjectsOutletContext } from "./MaintainedObjectsLayout";
import { ObjectDetailPanel } from "./ObjectDetailPanel";

export const ObjectDetailDrawer = () => {
  const { objectId } = useParams<{ objectId: string }>();
  const navigate = useNavigate();
  const { data, isLoading } =
    useOutletContext<MaintainedObjectsOutletContext>();

  const item = data.find((o) => o.id === objectId) ?? null;

  const handleClose = () => navigate("..", { relative: "path" });

  return (
    <SideDrawer
      isOpen
      onClose={handleClose}
      title={item?.name}
      width="66%"
      trapFocus={false}
    >
      {item ? (
        <ObjectDetailPanel item={item} />
      ) : (
        <Center py={10}>
          {isLoading ? <Spinner data-testid="loading-spinner" /> : null}
        </Center>
      )}
    </SideDrawer>
  );
};

export default ObjectDetailDrawer;
