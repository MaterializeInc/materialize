# Worksheet cell layouts

The worksheet spike gives each cell a SQL editor, results, and a handful of
controls: run or stop, live (wrap in `SUBSCRIBE`), table or chart, the cell's
name and status, a menu, and delete. Where those controls live decides
whether the page reads as a SQL worksheet or as a notebook. A fixed column of
controls to the right of each query was tried and felt clunky: it spends a
200px column on six small icons.

These are the alternatives considered. A with B's gutter is being tried first.

## A. Results toolbar

The query runs the full width. The controls sit in a slim bar between the
query and its results, where hosted SQL editors put result actions.

```
  SELECT cluster, count(*) AS replicas
  FROM {{cluster_replicas}} GROUP BY cluster
 ▶ ⚡ ▦   cell_1 · ● 4 rows · as of 12:04:31          ⋯ ×
  cluster        replicas
  quickstart     1
```

Run, live and the table or chart toggle sit next to what they act on. A cell
that has not run shows only the bar.

## B. Gutter run button, footer details

Like a desktop SQL IDE's per-statement gutter run button. The gutter holds ▶, or stop
while running, beside the first line. Name, status and the menu move to a
quiet line under the query.

```
▶ │ SELECT cluster, count(*) AS replicas
  │ FROM {{cluster_replicas}} GROUP BY cluster
  │ cell_1 · ● 4 rows                     ⚡ ▦ ⋯
```

The most compact option, and the query reads like a plain editor.

## C. Hover toolbar

Like common notebook editors. A small floating toolbar appears at a
cell's top right on hover or focus. At rest only the query, its results and a
status dot in the gutter show. The cleanest look, at the cost of
discoverability and touch use.

## D. One editor, many statements

What "SQL worksheet" means in most SQL editors. No cells: one
SQL document, a gutter ▶ per statement, and a results pane below with a tab
per statement, each switchable between table and chart.

```
▶ SELECT * FROM orders;
▶ SUBSCRIBE (SELECT count(*) FROM orders);   ⚡
  ─────────────────────────────────────────
  [ orders ] [ count ● live ]   ▦ table  📈 chart
  id  amount ...
```

The most familiar to SQL users, but a different product shape from cells.
`{{name}}` references would need a convention such as a `-- name: totals`
comment, Present would show pinned result tabs, and the two-column layout and
per-cell collapse would go away.

## E. Notebook

Closer to a notebook. Each cell shows line numbers and keeps its collapse and
run controls in a gutter beside them. The toolbar and results are indented to
start where the code does. Cells are told apart by a bar on their left rather
than borders, which highlights for the cell being edited and turns green
while a live cell runs. There is no separate composer: the worksheet always
ends in an empty cell, and running it turns it into a real cell and leaves a
fresh empty one below.

```
▌ ⌄ ▶ 1  SELECT c.name AS cluster, r.name AS replica
▌     2  FROM mz_clusters c JOIN mz_cluster_replicas r ON ...
▌        ⚡ 👁 ▦  cluster_replicas  SELECT 9                ⋯ ×
▌        cluster        replica
▌        quickstart     r1

▌   ▶ 1  Write SQL and press Shift-Enter to add it as a cell
▌        ⚡ ▦  ↑ recalls earlier queries               History ⌘G
```

## F. Results pane

Like a hosted SQL editor's worksheets. Cells hold only their query, name and status.
Results show in one resizable pane at the bottom, with a tab per cell that
has run. Running a cell selects its tab. The pane carries what acts on the
results: live, table or chart and the chart's settings, and whether the cell
appears on the dashboard. Present becomes Dashboard, which shows the cells not
hidden from it and offers full screen as a separate step.

## Status

- A with B's gutter: tried. Its toolbar was not indented with the code.
- E: built on A's toolbar and B's gutter.
- F: being tried, on top of E's gutter and line numbers.
- C: worth trying for its cleanliness.
- D: likely more familiar to users, but not the direction wanted for now.
