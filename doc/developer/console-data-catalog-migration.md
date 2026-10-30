# Data Catalog: Migration from Object Explorer

Status as of 2026-04-08. Tracks the work to replace the old object explorer
(`/objects` routes in `src/platform/object-explorer/`) with the inline data
catalog panel (`src/platform/worksheet/Catalog*.tsx`).

## What the catalog already supports

- **Browse tree** (Database > Schema > Object) with search filtering
- **Inline column preview** when expanding an object in the tree
- **Detail view** with columns, indexes, upstream/downstream dependencies, SHOW CREATE SQL
- **Live data** via per-section SUBSCRIBE WITH (PROGRESS) for columns, indexes, and dependencies
- **Cluster badge links** that navigate to the cluster detail page
- **System catalog objects** (mz_catalog, mz_internal, etc.)
- **Deep linking** from cluster page: clicking an MV opens its catalog detail; clicking an index opens the relation it indexes
- **Resizable sidebar** with drag handle (200px - 600px)
- **Object types**: table, view, materialized-view, source, sink, connection, secret, index (all types the old explorer handles)

## What still needs to be ported

### P0 - Blocks removing the old object explorer

#### ~~Source/sink creation redirects~~ DONE
All five creation flows now use `useOpenCatalogDetail` to navigate to the
catalog detail view after source creation instead of the old object explorer.

#### ~~Source list and sink list navigation~~ DONE
Clicking a source or sink in the list pages now opens the catalog detail view
via `useOpenCatalogDetail`. The "View workflow" menu item in SourcesList still
uses `useBuildSourcePath` (P1 dataflow visualizer migration).

#### ~~Source overview and errors~~ DONE
Ported in `dc00e561dd`. The catalog detail view for sources now shows an
"Overview" button that opens the full overview (with Overview + Errors tabs)
in the main content area via `ConnectorOverviewPage.tsx`. Reuses existing
`SourceOverview` and `SourceErrors` components without modification.

#### ~~Sink overview and errors~~ DONE
Ported in the same commit (`dc00e561dd`). Same approach as sources — the
"Overview" button in the catalog detail view opens `SinkOverview` or
`SinkErrors` in the main content area.

#### Object metadata in detail header
The old explorer shows **owner** and **creation date** for every object. The
catalog detail header currently shows: name, type badge, cluster badge, and
description. Owner and creation date should be added.

Query: `buildObjectDetailsQuery` in `src/api/materialize/object-explorer/objectDetails.ts`
selects owner and createdAt from `mz_roles` and `mz_object_lifetimes`.

#### Connection dependencies
When viewing a connection, the old explorer shows a table of all sources and
sinks using it. The catalog's generic dependency section doesn't surface this.

Query: `buildConnectionDependenciesQuery` in
`src/api/materialize/object-explorer/connectionDependencies.ts`.

### P1 - Important but not blocking

#### Object type filter
The old explorer has multi-select checkboxes to filter by type (table, view,
source, etc.). The catalog only has name search. Adding type filter chips or
a dropdown would improve browse UX.

#### Dataflow visualizer links
The old explorer links to the dataflow visualizer from MV and index detail
pages. The cluster page overflow menus still reference old paths:
- `IndexList.tsx` line ~288: `${indexPath(i)}/dataflow-visualizer`
- `MaterializedViewsList.tsx` line ~292: `${materializedViewPath(v)}/dataflow-visualizer`

Also referenced from:
- `src/platform/clusters/LargestMaintainedQueries.tsx` via `useBuildWorkflowGraphPath`

#### ~~Workflow graph sidebar~~ DONE
`src/components/WorkflowGraph/Sidebar.tsx` now uses `useOpenCatalogDetail` for
both source and sink node links.

### P2 - Nice to have

#### Database/schema detail views
The old explorer shows owner and creation date for databases and schemas, plus
a delete option for schemas. The catalog currently treats databases and schemas
as tree grouping nodes only.

#### Toast for missing objects
`useToastIfObjectNotExtant` shows a notification when a linked object no longer
exists. The catalog currently shows nothing special for deleted objects.

## References to old object explorer paths

All path builders are defined in `src/platform/routeHelpers.ts`:

| Function | Lines | Purpose |
|----------|-------|---------|
| `objectExplorerPath` | 80 | Base `/objects` path |
| `objectExplorerObjectPath` | 86 | Full object detail path |
| `useBuildObjectPath` | 92 | Generic object path hook |
| `useBuildMaterializedViewPath` | 105 | MV detail path (partially migrated) |
| `useBuildIndexPath` | 115 | Index detail path (partially migrated) |
| `useBuildSourcePath` | 125 | Source detail path |
| `useBuildSinkPath` | 135 | Sink detail path |
| `useBuildWorkflowGraphPath` | 154 | Workflow graph path |

The `useOpenCatalogDetail` hook in `src/store/catalog.ts` is the replacement.
It sets the catalog detail atom, opens the panel, and navigates to `/shell`.

## Shared code that must be preserved

These modules in `src/platform/object-explorer/` are imported by the catalog
and other parts of the app. They should be moved to a shared location (e.g.
`src/api/materialize/` or `src/components/`) before deleting the old explorer:

| Module | Exports used externally |
|--------|------------------------|
| `constants.ts` | `NULL_DATABASE_NAME` |
| `icons.ts` | `objectIcon` |
| `ObjectExplorerNode.ts` | `SupportedObjectType` |

Query builders in `src/api/materialize/object-explorer/` (objectColumns,
objectIndexes, objectDetails, connectionDependencies, etc.) are already in the
API layer and can stay.

## Route to remove

In `src/platform/AuthenticatedRoutes.tsx` lines 277-287:

```tsx
<Route path="objects">
  <Route index path="*" element={
    <BaseLayout sectionNav={<ObjectExplorerRoutes />}>
      <ObjectExplorerDetailRoutes />
    </BaseLayout>
  } />
</Route>
```

## Suggested migration order

1. Add owner + creation date to catalog detail header (unblocks metadata parity)
2. Add connection dependencies section to catalog detail view
3. Port source overview/errors (biggest effort, most-used pages)
4. Port sink overview/errors
5. Update source/sink creation flows to use `useOpenCatalogDetail`
6. Update source/sink list pages to use `useOpenCatalogDetail`
7. Update workflow graph sidebar link
8. Update dataflow visualizer links
9. Move shared modules (`NULL_DATABASE_NAME`, `objectIcon`, `SupportedObjectType`) out of `object-explorer/`
10. Remove old object explorer routes and components
