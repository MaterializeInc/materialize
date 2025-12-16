<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

# Stylesheet

The following page works as a quick visual demo of how we render common
HTML elements using our stylesheet.

# h1

## h2

### h3

#### h4

## h2 with `code`

| th  | th2  |
|-----|------|
| td  | td 2 |

**bold**

*underline*

`code`

[Linked `code`](/docs/self-managed/v25.2/)

<div class="highlight">

``` chroma
code block
```

</div>

[link](#h1)

**Unordered List:**

- ul1
- ul2
- ul3

**Ordered List:**

1.  ol1
2.  ol2
3.  ol3

When an ordered list has `<p>` tags (in markdown, multiple linebreaks
between items), it gets custom styles:

1.  This is an ordered list with paragraph, useful for step-by-step
    instructions.

2.  Testing out how this works.

## Shortcodes

### `diagram` shortcode

Diagrams from [rr.red-dove.com/ui](https://rr.red-dove.com/ui):

<div class="rr-diagram">

![](data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIzMzEiIGhlaWdodD0iMTg5Ij4KICAgPHBvbHlnb24gcG9pbnRzPSI5IDUgMSAxIDEgOSI+PC9wb2x5Z29uPgogICA8cG9seWdvbiBwb2ludHM9IjE3IDUgOSAxIDkgOSI+PC9wb2x5Z29uPgogICA8cmVjdCB4PSI3MSIgeT0iMjMiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI2OSIgeT0iMjEiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNzkiIHk9IjQxIj5GVUxMPC90ZXh0PgogICA8cmVjdCB4PSI3MSIgeT0iNjciIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI2OSIgeT0iNjUiIHdpZHRoPSI1NCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iNzkiIHk9Ijg1Ij5MRUZUPC90ZXh0PgogICA8cmVjdCB4PSI3MSIgeT0iMTExIiB3aWR0aD0iNjQiIGhlaWdodD0iMzIiIHJ4PSIxMCIgLz4KICAgPHJlY3QgeD0iNjkiIHk9IjEwOSIgd2lkdGg9IjY0IiBoZWlnaHQ9IjMyIiBjbGFzcz0idGVybWluYWwiIHJ4PSIxMCIgLz4KICAgPHRleHQgY2xhc3M9InRlcm1pbmFsIiB4PSI3OSIgeT0iMTI5Ij5SSUdIVDwvdGV4dD4KICAgPHJlY3QgeD0iMTk1IiB5PSI1NSIgd2lkdGg9IjY4IiBoZWlnaHQ9IjMyIiByeD0iMTAiIC8+CiAgIDxyZWN0IHg9IjE5MyIgeT0iNTMiIHdpZHRoPSI2OCIgaGVpZ2h0PSIzMiIgY2xhc3M9InRlcm1pbmFsIiByeD0iMTAiIC8+CiAgIDx0ZXh0IGNsYXNzPSJ0ZXJtaW5hbCIgeD0iMjAzIiB5PSI3MyI+T1VURVI8L3RleHQ+CiAgIDxyZWN0IHg9IjUxIiB5PSIxNTUiIHdpZHRoPSI2MiIgaGVpZ2h0PSIzMiIgcng9IjEwIiAvPgogICA8cmVjdCB4PSI0OSIgeT0iMTUzIiB3aWR0aD0iNjIiIGhlaWdodD0iMzIiIGNsYXNzPSJ0ZXJtaW5hbCIgcng9IjEwIiAvPgogICA8dGV4dCBjbGFzcz0idGVybWluYWwiIHg9IjU5IiB5PSIxNzMiPklOTkVSPC90ZXh0PgogICA8cGF0aCBjbGFzcz0ibGluZSIgZD0ibTE3IDUgaDIgbTIwIDAgaDEwIG0wIDAgaDI0MiBtLTI3MiAwIGgyMCBtMjUyIDAgaDIwIG0tMjkyIDAgcTEwIDAgMTAgMTAgbTI3MiAwIHEwIC0xMCAxMCAtMTAgbS0yODIgMTAgdjEyIG0yNzIgMCB2LTEyIG0tMjcyIDEyIHEwIDEwIDEwIDEwIG0yNTIgMCBxMTAgMCAxMCAtMTAgbS0yNDIgMTAgaDEwIG01NCAwIGgxMCBtMCAwIGgxMCBtLTEwNCAwIGgyMCBtODQgMCBoMjAgbS0xMjQgMCBxMTAgMCAxMCAxMCBtMTA0IDAgcTAgLTEwIDEwIC0xMCBtLTExNCAxMCB2MjQgbTEwNCAwIHYtMjQgbS0xMDQgMjQgcTAgMTAgMTAgMTAgbTg0IDAgcTEwIDAgMTAgLTEwIG0tOTQgMTAgaDEwIG01NCAwIGgxMCBtMCAwIGgxMCBtLTk0IC0xMCB2MjAgbTEwNCAwIHYtMjAgbS0xMDQgMjAgdjI0IG0xMDQgMCB2LTI0IG0tMTA0IDI0IHEwIDEwIDEwIDEwIG04NCAwIHExMCAwIDEwIC0xMCBtLTk0IDEwIGgxMCBtNjQgMCBoMTAgbTQwIC04OCBoMTAgbTAgMCBoNzggbS0xMDggMCBoMjAgbTg4IDAgaDIwIG0tMTI4IDAgcTEwIDAgMTAgMTAgbTEwOCAwIHEwIC0xMCAxMCAtMTAgbS0xMTggMTAgdjEyIG0xMDggMCB2LTEyIG0tMTA4IDEyIHEwIDEwIDEwIDEwIG04OCAwIHExMCAwIDEwIC0xMCBtLTk4IDEwIGgxMCBtNjggMCBoMTAgbS0yNDIgLTQyIHYyMCBtMjcyIDAgdi0yMCBtLTI3MiAyMCB2MTEyIG0yNzIgMCB2LTExMiBtLTI3MiAxMTIgcTAgMTAgMTAgMTAgbTI1MiAwIHExMCAwIDEwIC0xMCBtLTI2MiAxMCBoMTAgbTYyIDAgaDEwIG0wIDAgaDE3MCBtMjMgLTE2NCBoLTMiIC8+CiAgIDxwb2x5Z29uIHBvaW50cz0iMzIxIDUgMzI5IDEgMzI5IDkiPjwvcG9seWdvbj4KICAgPHBvbHlnb24gcG9pbnRzPSIzMjEgNSAzMTMgMSAzMTMgOSI+PC9wb2x5Z29uPgo8L3N2Zz4=)

</div>

### `note` shortcode

<div class="note">

**NOTE:** This is a note.

</div>

### `tip` shortcode

<div class="tip">

**💡 Tip:** This is a tip.

</div>

### `warning` shortcode

<div class="warning">

**WARNING!** This is a warning.

</div>

### `private-preview` shortcode

<div class="private-preview">

**PREVIEW** This is a private preview notice. is in **[private
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.  
  
To enable this feature in your Materialize region, [contact our
team](https://materialize.com/docs/support/).

</div>

### `public-preview` shortcode

<div class="public-preview">

**PREVIEW** This is a public preview notice. is in **[public
preview](https://materialize.com/preview-terms/)**. It is under active
development and may have stability or performance issues. It isn't
subject to our backwards compatibility guarantees.

</div>

### `cta` shortcode

<a href="/docs/self-managed/v25.2/get-started" class="cta">This is a CTA
button</a> <a href="/docs/self-managed/v25.2/get-started"
class="cta full_width">This is a “full_width” CTA button</a>

### `callout` shortcode

Used for prominent messages. Optionally can include a primary and
secondary CTA button using `primary_url`, `primary_text`,
`secondary_url`, `secondary_text` in shortcode params.

**Primary Only:**

<div class="callout">

<div>

# Header

Some text and the closing button is specified in the shortcode top.

<a href="/docs/self-managed/v25.2/get-started/" class="btn">Get
Started</a>

</div>

</div>

**Primary and Secondary:**

<div class="callout">

<div>

# Header

This example has two buttons!

<a href="/docs/self-managed/v25.2/" class="btn">Primary</a>
<a href="/docs/self-managed/v25.2/" class="btn">Secondary</a>

</div>

</div>

### `linkbox` shortcode

Used to render categorized lists of links, most helpful for “routing”
pages like the homepage.

<div class="linkbox bulb">

<div class="title">

Linkbox One

</div>

- [Link One](/docs/self-managed/v25.2/)
- [Second Link](/docs/self-managed/v25.2/)
- [Link number three](/docs/self-managed/v25.2/)

</div>

### `multilinkbox` shortcode

Group multiple linkboxes together to form a grid:

<div class="multilinkbox">

<div class="linkbox bulb">

<div class="title">

Linkbox One

</div>

- [Link One](/docs/self-managed/v25.2/)
- [Second Link](/docs/self-managed/v25.2/)
- [Link number three](/docs/self-managed/v25.2/)

</div>

<div class="linkbox book">

<div class="title">

Linkbox Two

</div>

- [Link One](/docs/self-managed/v25.2/)
- [Second Link](/docs/self-managed/v25.2/)
- [Link number three](/docs/self-managed/v25.2/)

</div>

<div class="linkbox doc">

<div class="title">

Linkbox Three

</div>

- [Link One](/docs/self-managed/v25.2/)
- [Second Link](/docs/self-managed/v25.2/)
- [Link number three](/docs/self-managed/v25.2/)

</div>

<div class="linkbox touch">

<div class="title">

Linkbox Four

</div>

- [Link One](/docs/self-managed/v25.2/)
- [Second Link](/docs/self-managed/v25.2/)
- [Link number three](/docs/self-managed/v25.2/)

</div>

</div>

### `tabs` shortcode

<div class="code-tabs">

<div class="tab-content">

<div id="tab-tab-1" class="tab-pane" title="Tab 1">

block1 Tab1

</div>

<div id="tab-tab-2" class="tab-pane" title="Tab 2">

block1 Tab2

</div>

</div>

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-tab-1" class="tab-pane" title="Tab 1">

block2 Tab1

</div>

<div id="tab-tab-2" class="tab-pane" title="Tab 2">

Block2 Tab2

</div>

</div>

</div>

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/stylesheet.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
