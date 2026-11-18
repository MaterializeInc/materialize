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

import { isSystemId } from "~/api/materialize";
import { SideDrawer } from "~/components/SideDrawer";
import {
  EmptyListHeader,
  EmptyListHeaderContents,
  EmptyListWrapper,
} from "~/layouts/listPageComponents";
import { useAllObjects } from "~/store/allObjects";

import { MaintainedObjectsOutletContext } from "./MaintainedObjectsLayout";
import { ObjectDetailPanel } from "./ObjectDetailPanel";
import { ObjectJourneyBreadcrumb } from "./ObjectJourneyBreadcrumb";

const UNLISTED_MESSAGES = {
  writableTable: {
    title: "No freshness data for this table",
    helpText:
      "This table is written with INSERT, so it changes only when someone writes to it and has nothing to fall behind. This page tracks objects Materialize keeps up to date: sources, materialized views, indexes, sinks, and tables fed by a source.",
  },
  untracked: {
    title: "This page doesn't track this object",
    helpText:
      "It tracks objects Materialize keeps up to date: the sources, materialized views, indexes, sinks, and source-fed tables you create.",
  },
  missing: {
    title: "Object not found",
    helpText: "It may have been dropped, or the link may be wrong.",
  },
};

/**
 * Why an object this page does not list has no drawer. A link from elsewhere
 * in the Console can still name one, most often a writable table from the
 * Freshness page.
 */
const UnlistedState = ({ objectId }: { objectId: string | undefined }) => {
  const { data: allObjects } = useAllObjects();
  const object = allObjects.find((o) => o.id === objectId);
  // A system table is a table too, but it is not one anyone writes to.
  const reason = !object
    ? "missing"
    : object.objectType === "table" && !isSystemId(object.id)
      ? "writableTable"
      : "untracked";
  return (
    <EmptyListWrapper>
      <EmptyListHeader>
        <EmptyListHeaderContents {...UNLISTED_MESSAGES[reason]} />
      </EmptyListHeader>
    </EmptyListWrapper>
  );
};

export const ObjectDetailDrawer = () => {
  const { objectId } = useParams<{ objectId: string }>();
  const navigate = useNavigate();
  const {
    data,
    isLoading,
    hydrationReady,
    isError,
    lookbackMinutes,
    setLookbackMinutes,
  } = useOutletContext<MaintainedObjectsOutletContext>();

  const item = data.find((o) => o.id === objectId) ?? null;

  const handleClose = () => navigate("..", { relative: "path" });

  return (
    <SideDrawer
      isOpen
      onClose={handleClose}
      title={
        item ? (
          <ObjectJourneyBreadcrumb
            currentId={item.id}
            currentName={item.name}
          />
        ) : undefined
      }
      width="66%"
      trapFocus={false}
    >
      {item ? (
        <ObjectDetailPanel
          item={item}
          lookbackMinutes={lookbackMinutes}
          setLookbackMinutes={setLookbackMinutes}
        />
      ) : (
        <Center py={10}>
          {/* A source-fed table is only listed once the hydration snapshot
              names its source. */}
          {isLoading || (!hydrationReady && !isError) ? (
            <Spinner data-testid="loading-spinner" />
          ) : isError ? null : (
            <UnlistedState objectId={objectId} />
          )}
        </Center>
      )}
    </SideDrawer>
  );
};

export default ObjectDetailDrawer;
