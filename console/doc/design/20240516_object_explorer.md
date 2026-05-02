# Object Explorer

- [Figma](https://www.figma.com/design/T0wCoze1wCXlgyDc884Jpz/Console-Q224?node-id=1-6)
- [Epic](https://github.com/MaterializeInc/console/issues/1820)
- [Product
  brief](https://www.notion.so/materialize/Improving-data-base-discovery-in-the-console-7e4e01e731fb44d3bb1d5c77cd247e3d?pvs=4)

## The Problem

The Materialize console, originally designed with a Cluster-centric approach to it’s UX
(before multi-purpose clusters), no longer meets the needs of our users due to
significant changes in core concepts, particularly around multipurpose clusters.
Additionally, we have gained insights as to how customers use the product in practice
from a number of customer use cases that highlight gaps in the existing product’s user
experience.

## Success Criteria

User can easily discover and navigate their databases, schemas and objects in a familiar
way.

## Out of Scope

The Figma designs include a permission table, but that's not in scope for the initial
version, which is the focus of this document.

## Solution Proposal

### Object tree view

The tree view is something brand new for Console and has the most open questions.

After surveying different options for rendering the tree of objects, it seems like
rolling our own is probably the best move. Most libraries in this space are either very
large and have tons of features (e.g. drag and drop) that we don't need, or they render
using d3, designed more for rendering arbitrary graphs.

It should be fairly simple to build this tree view using `ol`, `li`, `details` and
`summary` tags. A rough sketch of the component tree:

```tsx
<TreeNode title={database.name} icon={<DatabaseIcon />}>
  <TreeNode title={schema.name} icon={<SchemaIcon />}>
    <TreeNode title="Connections">
      <TreeNode title={object.name} icon={<ConnectionIcon} />
    </TreeNode>
  </TreeNode>
</TreeNode>
```

Imagine that each layer is mapping over all the objects in that layer of the tree.

Initially the tree will be collapsed except the selected database. Once a user starts
expanding the tree, there is some concern about rendering performance. We have some
customers with thousands of objects, if those are all represented as a set of DOM nodes,
performance will suffer. The only planned mitigation is a "show more" button, if a given
node has more than 50 objects, we only show the first 50, and present a button that will
reveal the rest. In general, users with lots of objects should be using schemas to
organize them, which helps keep the number of children for a given node down.

If the performance of this approach is poor in real world scenarios, we can consider
virtualizing the tree view, possibly using the `react-vtree` library, which is built on
top of `react-window`, which we already use for the shell. This would be an enhancement,
not part of the initial v1 of this feature.

#### Data fetching

For all cases, we will need to fetch the full list of schemas and databases up front,
since those will all be displayed.

```sql
SELECT s.id, s.database_id, s.name, d.name AS database_name
FROM mz_schemas AS s LEFT JOIN mz_databases AS d ON d.id = s.database_id;
```

On the client we will split these into system and user schemas, system schemas will be
displayed in a separate pane at the bottom of the tree view.

For the actual object data, we have a few options.

1. Fetch all the object data up front, the simplest approach.

   ```sql
   SELECT id, name, object_type, schema_id, schema_name, database_id, database_name
   FROM mz_internal.mz_object_fully_qualified_names
   WHERE id LIKE 'u%';
   ```

   For some of our largest customer (by number of objects), this query takes 100 - 300ms.

2. Subscribe to all objects on app load. This has benefits beyond this feature, we could
   use this data to power our smart redirect feature, which handles dropped and recreated
   objects as well as renamed objects for all object detail pages. It also means if you
   initially load any page besides the object explorer pages, the data will already be
   loaded by the time you visit the object explorer. The query would be the same as
   option 1, just wrapped in subscribe. This seems like the best option to me, since it
   solves several problems at once, and should have the best perceived performance.

   There is some concern about the performance of e.g. a dbt run that drops and creates
   thousands of objects. Since our `useSubscribe` hook buffers updates and flushes on an
   interval, even this scenario should not degrade performance much. The cluster metric
   graphs essentially do the same thing, on load for the 30 day view they stream in
   ~43,200 rows and render on every buffer flush (currently every 16ms). We can also tune
   the buffer flush interval down for this case, since we don't need it to appear smooth
   in the same way as the graph line being drawn.

3. Fetch the data for each schema separately when the user expands it. This would improve
   the initial load time slightly compared to option 1, and the full page load for object
   explorer pages with option 2. It also makes the interface more resilient to
   environments with many thousands of objects. However, even for a schema with few
   objects in it, this query take ~100ms, which is enough to make the interface feel a
   bit sluggish.

   ```sql
   SELECT id, name, object_type, schema_id, schema_name, database_id, database_name
   FROM mz_internal.mz_object_fully_qualified_names
   WHERE schema_id = $1
   AND id LIKE 'u%';
   ```

After some discussion, we feel that option 2 is best, it gives us fresh data and a snappy
experience when navigating through the object explorer.

#### Object name filter

A simple text input that allows users to filter by name. Since we plan to at least fetch
all the objects for a schema at once, it's easy enough to handle the filtering client
side.

#### Object group by

A button next to the name filter allows users to switch between grouping by database
object hierarchy or object type. The default is object hierarchy. When switching to type,
we will not show databases and schemas, instead we will just show tables, views,
materialized views, indexes and sources. In order to implement this feature, we have to
either load all objects up front, or when the user toggles this option, we could load
objects by type, as the user expands the nodes. Looking at the common patterns with
materialize, many large customers will have thousands of views. This seems like another
reason to lean towards loading everything up front, since the perceived performance of
loading each type separately will be worse than one slightly slower loading state when
they first visit the page.

### Database page

The details page will need to fetch some additional data.

```sql
SELECT d.id, d.name, ol.occurred_at, r.name
FROM
    mz_databases AS d
        JOIN mz_internal.mz_object_lifetimes AS ol USING(id)
        JOIN mz_roles AS r ON d.owner_id = r.id
WHERE ol.event_type = 'create' AND d.id = $1;
```

### Schema page

Similar to the database details.

```sql
SELECT s.id, s.name, ol.occurred_at, r.name
FROM
    mz_schemas AS s
        JOIN mz_internal.mz_object_lifetimes AS ol USING(id)
        JOIN mz_roles AS r ON s.owner_id = r.id
WHERE ol.event_type = 'create' AND s.id = $1;
```

### Object details page

This will be a single component used for every object type.

The query is similar to the above queries, but for schema objects.

```sql
SELECT o.id, o.name, o.type, o.cluster_id, ol.occurred_at, r.name
FROM
    mz_objects AS o
        JOIN mz_internal.mz_object_lifetimes AS ol USING(id)
        LEFT JOIN mz_clusters AS ol USING(id)
        JOIN mz_roles AS r ON o.owner_id = r.id
WHERE ol.event_type = 'create' AND o.id = $1;
```

Note: `mz_objects` currently doesn't include the cluster ID, I'm planning on adding it.

Also, these pages will display the DDL if it's available.

```sql
SHOW CREATE <object_type> $1;
```

### Object columns tab

Every object that has columns will have a columns tab that will show the column metadata.

```sql
SELECT name, position, nullable, type FROM mz_columns WHERE id = 'u182';
```

### Deleted objects

If a user is looking at an object that is deleted out from under them, we should not
immediately redirect them to another node, since this would be disorienting. Instead, we
will show a notice that tells them the object was deleted. I think the easiest way to get
this behavior is to make the details query for an object throw a custom error if the
database returns no rows, then special case that error in the component to show the
notice, rather than a generic error boundary.

One open question is if we should bother continuing to show the object(s) that were
deleted in the tree. This quite a bit more complex, because can't treat an item being
removed as an error and use the RQ cache. Instead, we would have to maintain state on top
of the raw data that includes the currently selected item, even if it's deleted.
Furthermore, the entire schema or database might have been cascade dropped, so we would
have to somehow keep track of the tree of objects that was removed. I don't think this is
worth the additional complexity, we can just let the object disappear and make sure the
tree handles having no selected value.

## Minimal Viable Prototype

See the [Figma](https://www.figma.com/design/T0wCoze1wCXlgyDc884Jpz/Console-Q224?node-id=1-6) design.

## Open questions

We may have to virtualize the tree view rendering, but we won't really know until we
built it and do some stress testing.
