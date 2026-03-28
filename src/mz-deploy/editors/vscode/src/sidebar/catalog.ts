/**
 * Catalog sidebar webview script — browsable data catalog for the mz-deploy project.
 *
 * ## Modes
 *
 * The UI switches between two modes based on whether `navStack` is empty:
 *
 * - **Browse mode** (`navStack.length === 0`) — A tree organized by
 *   database > schema > objects. Each object row is expandable to show an
 *   inline column preview with PK/FK indicators. A search bar filters objects
 *   across name, schema, database, and type. An inspect button on each row
 *   navigates to detail mode.
 *
 * - **Detail mode** (`navStack.length > 0`) — Full inspection of a single
 *   object with sections for columns (with nullability, PK/FK badges, and FK
 *   navigation links), indexes, constraints, grants, dependencies (objects
 *   this depends on), referenced-by (objects that depend on this), and
 *   infrastructure (connector, connection, source, properties). A breadcrumb
 *   bar supports back-navigation through the drill-down stack.
 *
 * ## Data Flow
 *
 *     extension host ──postMessage──► "catalog-data" ──► catalogData, objectMap
 *                                     "inspect-object" ──► navStack reset
 *     webview ──postMessage──► "open-file"   (open SQL source in editor)
 *                              "open-dag"    (open DAG panel focused on object)
 *                              "ready"       (handshake to receive cached data)
 *
 * ## DOM Construction
 *
 * All DOM is built imperatively via the `h()` helper (a minimal
 * `createElement` wrapper). The entire tree is re-rendered on every state
 * change by clearing `#root` and rebuilding. This keeps the rendering logic
 * stateless and easy to follow at the cost of DOM diffing — acceptable for
 * the small DOM sizes involved in a sidebar panel.
 *
 * **Key Insight:** After a re-render in browse mode with an active search
 * query, the search input is re-focused and the cursor repositioned to the
 * end so typing is uninterrupted.
 */

declare function acquireVsCodeApi(): { postMessage(msg: unknown): void };

(function () {

  interface Column {
    name: string;
    type_name: string;
    nullable?: boolean;
    comment?: string;
  }

  interface Constraint {
    kind: string;
    name: string;
    columns: string[];
    references?: string;
  }

  interface Index {
    name: string;
    columns: string[];
    cluster?: string;
  }

  interface Grant {
    privilege: string;
    role: string;
  }

  interface Property {
    key: string;
    value?: string;
    secret_ref?: string;
    object_ref?: string;
  }

  interface Infrastructure {
    connector_type?: string;
    connection_ref?: string;
    source_ref?: string;
    properties?: Property[];
  }

  interface CatalogObject {
    id: string;
    name: string;
    schema: string;
    database: string;
    object_type: string;
    file_path?: string;
    columns?: Column[];
    constraints?: Constraint[];
    indexes?: Index[];
    grants?: Grant[];
    dependencies: string[];
    dependents: string[];
    infrastructure?: Infrastructure;
    is_external?: boolean;
    description?: string;
    cluster?: string;
  }

  interface SchemaEntry {
    name: string;
    object_ids: string[];
  }

  interface DatabaseEntry {
    name: string;
    schemas: SchemaEntry[];
  }

  interface CatalogError {
    message: string;
  }

  interface CatalogData {
    databases: DatabaseEntry[];
    objects: CatalogObject[];
    errors?: CatalogError[];
  }

  type CatalogInboundMessage =
    | { type: "catalog-data"; data: CatalogData }
    | { type: "inspect-object"; id: string };

  type CatalogOutboundMessage =
    | { type: "open-file"; path: string }
    | { type: "open-dag"; focusTable?: string }
    | { type: "ready" };

  // VS Code webview API
  const vscode = acquireVsCodeApi();

  // --- State ---
  let catalogData: CatalogData | null = null; // { databases, objects }
  let objectMap: Record<string, CatalogObject> = {};     // id -> object (built from catalogData.objects)
  let search: string = "";
  let expandedDatabases: Set<string> = new Set();
  let expandedObjects: Set<string> = new Set();
  let navStack: { id: string }[] = [];      // [{ id: "db.schema.name" }, ...]
  let hoveredColumn: string | null = null;

  // --- Icons (inline SVG) ---
  const ICONS: Record<string, string> = {
    key: `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#C9962A" stroke-width="2.5" stroke-linecap="round"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 11-7.778 7.778 5.5 5.5 0 017.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg>`,
    fk: `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#5B9BD5" stroke-width="2.5" stroke-linecap="round"><path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"/></svg>`,
    table: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="3" x2="9" y2="21"/></svg>`,
    view: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>`,
    source: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>`,
    sink: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 2v20"/><path d="M5 15l7 7 7-7"/></svg>`,
    connection: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="5" r="3"/><circle cx="12" cy="19" r="3"/><line x1="12" y1="8" x2="12" y2="16"/></svg>`,
    secret: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>`,
    search: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>`,
    chevRight: `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M9 18l6-6-6-6"/></svg>`,
    chevDown: `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M6 9l6 6 6-6"/></svg>`,
    inspect: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>`,
    back: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><path d="M19 12H5"/><path d="M12 19l-7-7 7-7"/></svg>`,
    dag: `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="5" cy="6" r="3"/><circle cx="19" cy="6" r="3"/><circle cx="12" cy="18" r="3"/><line x1="7.5" y1="7.5" x2="10.5" y2="15.5"/><line x1="16.5" y1="7.5" x2="13.5" y2="15.5"/></svg>`,
    index: `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#8BB83F" stroke-width="2.5" stroke-linecap="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>`,
    check: `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#6A8A3A" stroke-width="2.5" stroke-linecap="round"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg>`,
    goToFile: `<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="9" y1="15" x2="15" y2="15"/><polyline points="13 13 15 15 13 17"/></svg>`,
    database: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 5v14c0 1.66-4.03 3-9 3S3 20.66 3 19V5"/><path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"/></svg>`,
    schema: `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M3 3h7v7H3z"/><path d="M14 3h7v7h-7z"/><path d="M3 14h7v7H3z"/></svg>`,
  };

  /** Maps an object type string to its corresponding inline SVG icon. */
  function objectIcon(type: string): string {
    switch (type) {
      case "view":
      case "materialized-view":
        return ICONS.view;
      case "source":
      case "table-from-source":
        return ICONS.source;
      case "sink":
        return ICONS.sink;
      case "connection":
        return ICONS.connection;
      case "secret":
        return ICONS.secret;
      default:
        return ICONS.table;
    }
  }

  /**
   * Minimal DOM element factory.
   *
   * @param {string} tag - HTML tag name
   * @param {Object|null} attrs - Attributes/properties. Special keys:
   *   `className` → `el.className`, `innerHTML` → `el.innerHTML`,
   *   `on*` → addEventListener, `style` (object) → Object.assign,
   *   `title` → `el.title`, all others → `setAttribute`.
   * @param {...(string|Node|null)} children - Text or DOM nodes to append.
   * @returns {HTMLElement}
   */
  function h(tag: string, attrs: Record<string, unknown> | null, ...children: (string | Node | null | undefined)[]): HTMLElement {
    const el = document.createElement(tag);
    if (attrs) {
      for (const [k, v] of Object.entries(attrs)) {
        if (k === "className") el.className = v as string;
        else if (k === "innerHTML") el.innerHTML = v as string;
        else if (k.startsWith("on")) el.addEventListener(k.slice(2).toLowerCase(), v as EventListener);
        else if (k === "style" && typeof v === "object") Object.assign(el.style, v);
        else if (k === "title") el.title = v as string;
        else el.setAttribute(k, v as string);
      }
    }
    for (const child of children) {
      if (child == null) continue;
      if (typeof child === "string") el.appendChild(document.createTextNode(child));
      else if (child instanceof Node) el.appendChild(child);
    }
    return el;
  }

  /** Resolved PK/FK constraint sets for an object's columns. */
  interface ConstraintSets {
    primaryKeys: Set<string>;
    /** Maps column name → referenced object ID. */
    foreignKeys: Map<string, string>;
  }

  /**
   * Extracts primary key and foreign key column sets from an object's constraints.
   * Used by both inline column preview (browse mode) and full column detail.
   */
  function resolveConstraintSets(object: CatalogObject): ConstraintSets {
    const primaryKeys = new Set<string>();
    const foreignKeys = new Map<string, string>();
    for (const constraint of object.constraints || []) {
      if (constraint.kind === "PRIMARY KEY") {
        constraint.columns.forEach((col) => primaryKeys.add(col));
      }
      if (constraint.kind === "FOREIGN KEY" && constraint.references) {
        constraint.columns.forEach((col) => foreignKeys.set(col, constraint.references!));
      }
    }
    return { primaryKeys, foreignKeys };
  }

  /** Creates a small colored badge span (e.g., "PK", "FK", "NOT NULL"). */
  function badge(label: string, color: string): HTMLElement {
    return h("span", { className: "badge", style: { color, background: color + "15" } }, label);
  }

  /**
   * Top-level render dispatcher. Clears `#root` and delegates to
   * {@link renderBrowseMode} or {@link renderDetailMode} based on `navStack`.
   */
  function render(): void {
    const root = document.getElementById("root")!;
    root.innerHTML = "";

    if (!catalogData) {
      root.appendChild(h("div", { className: "empty-state" }, "Waiting for project data..."));
      return;
    }

    if (catalogData.errors && catalogData.errors.length > 0) {
      renderBuildErrors(root);
      return;
    }

    if (navStack.length > 0) {
      renderDetailMode(root);
    } else {
      renderBrowseMode(root);
    }

    // Restore focus to search input after DOM rebuild
    if (navStack.length === 0 && search) {
      const input = root.querySelector(".search-input") as HTMLInputElement | null;
      if (input) {
        input.focus();
        input.setSelectionRange(search.length, search.length);
      }
    }
  }

  /** Renders build errors when the project failed to compile. */
  function renderBuildErrors(root: HTMLElement): void {
    const container = h("div", { className: "build-errors" });
    container.appendChild(h("div", { className: "build-errors-title" }, "Project build failed"));
    for (const err of catalogData!.errors!) {
      const card = h("div", { className: "build-error-card" });
      card.appendChild(h("pre", { className: "build-error-message" }, err.message));
      container.appendChild(card);
    }
    root.appendChild(container);
  }

  /**
   * Renders browse mode: search bar, then a tree of database > schema > object
   * rows. Objects are filtered by the current search query across name, schema,
   * database, and type. Schemas with no matching objects are hidden.
   */
  function renderBrowseMode(root: HTMLElement): void {
    // Search + actions (VS Code provides the panel title, so no header needed)
    const searchBox = h("div", { className: "search-box" },
      h("span", { className: "search-icon", innerHTML: ICONS.search }),
      h("input", {
        className: "search-input",
        placeholder: "Filter objects...",
        value: search,
        onInput: (e: Event) => { search = (e.target as HTMLInputElement).value; render(); },
      }),
      h("span", {
        className: "icon-btn",
        innerHTML: ICONS.dag,
        title: "Open DAG view",
        onClick: () => vscode.postMessage({ type: "open-dag" } satisfies CatalogOutboundMessage),
      }),
      h("span", { className: "header-count" },
        `${catalogData!.objects.length}`
      ),
    );
    root.appendChild(searchBox);

    // Tree
    const tree = h("div", { className: "tree" });

    for (const db of catalogData!.databases) {
      const dbExpanded = expandedDatabases.has(db.name);

      // Database header (always shown)
      tree.appendChild(
        h("div", {
          className: "database-header",
          onClick: () => { toggleDatabase(db.name); },
        },
          h("span", { className: "chevron", innerHTML: dbExpanded ? ICONS.chevDown : ICONS.chevRight }),
          h("span", { className: "database-icon", innerHTML: ICONS.database }),
          h("span", { className: "database-name" }, db.name)
        )
      );

      if (!dbExpanded) continue;

      for (const schema of db.schemas) {
        // Filter objects
        const objects = schema.object_ids
          .map((id) => objectMap[id])
          .filter(Boolean)
          .filter((obj) => {
            if (!search) return true;
            const q = search.toLowerCase();
            return (
              obj.name.toLowerCase().includes(q) ||
              obj.schema.toLowerCase().includes(q) ||
              obj.database.toLowerCase().includes(q) ||
              obj.object_type.toLowerCase().includes(q)
            );
          });

        if (objects.length === 0) continue;

        // Schema header
        tree.appendChild(
          h("div", { className: "schema-header" },
            h("span", { className: "schema-icon", innerHTML: ICONS.schema }),
            schema.name
          )
        );

        // Objects
        for (const obj of objects) {
          tree.appendChild(renderObjectRow(obj));

          // Expanded columns
          if (expandedObjects.has(obj.id) && obj.columns && obj.columns.length > 0) {
            tree.appendChild(renderInlineColumns(obj));
          }
        }
      }
    }

    root.appendChild(tree);
  }

  /** Renders a single object row with expand chevron, icon, name, and inspect button. */
  function renderObjectRow(obj: CatalogObject): HTMLElement {
    const isExpanded = expandedObjects.has(obj.id);
    const hasColumns = obj.columns && obj.columns.length > 0;

    // Objects without columns navigate directly to detail on name click.
    // Objects with columns toggle the inline column preview instead.
    const nameClick = hasColumns
      ? () => toggleExpand(obj.id)
      : () => drillInto(obj.id);

    const row = h("div", { className: "object-row" },
      h("span", {
        className: "chevron",
        innerHTML: hasColumns ? (isExpanded ? ICONS.chevDown : ICONS.chevRight) : "",
        onClick: (e: Event) => { e.stopPropagation(); toggleExpand(obj.id); },
      }),
      h("span", {
        className: "object-name",
        onClick: nameClick,
      },
        h("span", { className: "object-icon", innerHTML: objectIcon(obj.object_type) }),
        h("span", { className: "name-text" + (obj.is_external ? " external" : "") }, obj.name)
      ),
    );

    // Only show the inspect icon for objects with columns (tables, views, etc.).
    // Column-less objects (connections, secrets, sinks) navigate via name click.
    if (hasColumns) {
      row.appendChild(h("span", {
        className: "inspect-btn",
        innerHTML: ICONS.inspect,
        title: "Inspect " + obj.name,
        onClick: (e: Event) => { e.stopPropagation(); drillInto(obj.id); },
      }));
    }

    return row;
  }

  /**
   * Renders the expanded inline column preview for an object in browse mode.
   * Shows each column with PK (key icon, gold) or FK (link icon, blue)
   * indicators derived from the object's constraints. Includes a "View full
   * detail" link at the bottom.
   */
  function renderInlineColumns(obj: CatalogObject): HTMLElement {
    const container = h("div", { className: "inline-columns" });
    const { primaryKeys, foreignKeys } = resolveConstraintSets(obj);

    for (const column of obj.columns!) {
      const isPK = primaryKeys.has(column.name);
      const isFK = foreignKeys.has(column.name);

      container.appendChild(
        h("div", { className: "inline-col" },
          h("span", { className: "col-icon", innerHTML: isPK ? ICONS.key : isFK ? ICONS.fk : "" }),
          h("span", {
            className: "col-name",
            style: { color: isPK ? "#C9962A" : isFK ? "#5B9BD5" : "#5A5E6A" },
          }, column.name),
          h("span", { className: "col-type" }, column.type_name)
        )
      );
    }

    // "View full detail" link
    container.appendChild(
      h("div", {
        className: "detail-link",
        onClick: () => drillInto(obj.id),
      }, "View full detail \u2192")
    );

    return container;
  }

  /**
   * Renders detail mode for the object at the top of `navStack`.
   *
   * Sections rendered (in order):
   * 1. Breadcrumb bar with back button and clickable ancestors
   * 2. Object header (icon, name, type badge, open-file/DAG action buttons)
   * 3. Columns with PK/FK badges, nullability, comments, and FK drill-down
   * 4. Indexes with column lists and cluster assignments
   * 5. Constraints (non-PK/FK only — PK/FK shown as column badges)
   * 6. Grants (privilege → role mappings)
   * 7. Dependencies (clickable links to upstream objects)
   * 8. Referenced By (clickable links to downstream objects)
   * 9. Infrastructure (connector type, connection, source, properties)
   */
  function renderDetailMode(root: HTMLElement): void {
    const currentId = navStack[navStack.length - 1].id;
    const obj = objectMap[currentId];
    if (!obj) {
      root.appendChild(h("div", { className: "empty-state" }, "Object not found"));
      return;
    }

    // Breadcrumb bar
    const breadcrumb = h("div", { className: "breadcrumb-bar" },
      h("span", {
        className: "back-btn",
        innerHTML: ICONS.back,
        title: "Go back",
        onClick: goBack,
      }),
      h("span", {
        className: "breadcrumb-link",
        onClick: () => { navStack = []; render(); },
      }, "Catalog")
    );

    for (let i = 0; i < navStack.length; i++) {
      const item = navStack[i];
      const itemObj = objectMap[item.id];
      const isLast = i === navStack.length - 1;

      breadcrumb.appendChild(h("span", { className: "breadcrumb-sep" }, "\u203A"));

      if (isLast) {
        breadcrumb.appendChild(
          h("span", { className: "breadcrumb-current" },
            itemObj ? `${itemObj.schema}.${itemObj.name}` : item.id
          )
        );
      } else {
        breadcrumb.appendChild(
          h("span", {
            className: "breadcrumb-link",
            onClick: () => { navStack = navStack.slice(0, i + 1); render(); },
          }, itemObj ? `${itemObj.schema}.${itemObj.name}` : item.id)
        );
      }
    }

    root.appendChild(breadcrumb);

    // Object header
    const headerDiv = h("div", { className: "detail-header" },
      h("div", { className: "detail-title-row" },
        h("span", { className: "detail-icon", innerHTML: objectIcon(obj.object_type), style: { color: "#C9962A" } }),
        h("span", { className: "detail-name" }, obj.name),
        h("span", { className: "detail-type-badge" }, obj.object_type),
      ),
      h("div", { className: "detail-actions" },
        obj.file_path
          ? h("button", {
              className: "action-btn",
              onClick: () => vscode.postMessage({ type: "open-file", path: obj.file_path! } satisfies CatalogOutboundMessage),
              title: `Open ${obj.file_path}`,
            },
              h("span", { innerHTML: ICONS.goToFile }),
              "File"
            )
          : null,
        h("button", {
          className: "action-btn",
          onClick: () => vscode.postMessage({ type: "open-dag", focusTable: obj.id } satisfies CatalogOutboundMessage),
          title: `Show ${obj.name} in DAG`,
        },
          h("span", { innerHTML: ICONS.dag }),
          "DAG"
        ),
      )
    );

    if (obj.description) {
      headerDiv.appendChild(
        h("div", { className: "detail-desc" }, obj.description)
      );
    }

    if (obj.cluster) {
      headerDiv.appendChild(
        h("div", { className: "detail-cluster" },
          h("span", { className: "cluster-label" }, "cluster:"),
          ` ${obj.cluster}`
        )
      );
    }

    root.appendChild(headerDiv);

    // Scrollable content
    const content = h("div", { className: "detail-content" });

    // --- Columns ---
    if (obj.columns && obj.columns.length > 0) {
      content.appendChild(h("div", { className: "section-title" }, "Columns"));
      const { primaryKeys, foreignKeys } = resolveConstraintSets(obj);

      for (const column of obj.columns) {
        const isPK = primaryKeys.has(column.name);
        const isFK = foreignKeys.has(column.name);

        const colRow = h("div", {
          className: "detail-col-row" + (hoveredColumn === column.name ? " hovered" : ""),
          onMouseenter: () => { hoveredColumn = column.name; },
          onMouseleave: () => { hoveredColumn = null; },
        },
          h("div", { className: "detail-col-main" },
            h("span", { className: "col-icon", innerHTML: isPK ? ICONS.key : isFK ? ICONS.fk : "" }),
            h("span", {
              className: "detail-col-name",
              style: { color: isPK ? "#C9962A" : isFK ? "#5B9BD5" : "#A8AEBB" },
            }, column.name),
            h("span", { className: "detail-col-type" }, column.type_name)
          )
        );

        // Description
        if (column.comment) {
          colRow.appendChild(
            h("div", { className: "detail-col-desc" }, column.comment)
          );
        }

        // Badges
        const badges = h("div", { className: "detail-col-badges" });
        if (isPK) badges.appendChild(badge("PK", "#C9962A"));
        if (isFK) badges.appendChild(badge("FK", "#5B9BD5"));
        if (!column.nullable) badges.appendChild(badge("NOT NULL", "#5A5E6A"));
        if (badges.children.length > 0) colRow.appendChild(badges);

        // FK link
        if (isFK) {
          const refId = foreignKeys.get(column.name)!;
          colRow.appendChild(
            h("div", {
              className: "fk-link",
              onClick: () => drillInto(refId),
            },
              h("span", { innerHTML: ICONS.fk, style: { marginRight: "4px" } }),
              `\u2192 ${refId}`
            )
          );
        }

        content.appendChild(colRow);
      }
    }

    // --- Indexes ---
    if (obj.indexes && obj.indexes.length > 0) {
      content.appendChild(h("div", { className: "section-title section-border" }, "Indexes"));
      for (const index of obj.indexes) {
        content.appendChild(
          h("div", { className: "index-row" },
            h("span", { className: "index-icon", innerHTML: ICONS.index }),
            h("div", { className: "index-info" },
              h("div", { className: "index-name" }, index.name),
              h("div", { className: "index-cols" }, `(${index.columns.join(", ")})`)
            ),
            index.cluster
              ? h("span", { className: "index-cluster" }, index.cluster)
              : null
          )
        );
      }
    }

    // --- Constraints ---
    const otherConstraints = (obj.constraints || []).filter(
      (constraint) => constraint.kind !== "PRIMARY KEY" && constraint.kind !== "FOREIGN KEY"
    );
    if (otherConstraints.length > 0) {
      content.appendChild(h("div", { className: "section-title section-border" }, "Constraints"));
      for (const constraint of otherConstraints) {
        content.appendChild(
          h("div", { className: "constraint-row" },
            h("span", { innerHTML: ICONS.check }),
            h("span", { className: "constraint-text" },
              `${constraint.kind}: ${constraint.name} (${constraint.columns.join(", ")})`
            )
          )
        );
      }
    }

    // --- Grants ---
    if (obj.grants && obj.grants.length > 0) {
      content.appendChild(h("div", { className: "section-title section-border" }, "Grants"));
      for (const grant of obj.grants) {
        content.appendChild(
          h("div", { className: "grant-row" },
            h("span", { className: "grant-priv" }, grant.privilege),
            h("span", { className: "grant-arrow" }, "\u2192"),
            h("span", { className: "grant-role" }, grant.role)
          )
        );
      }
    }

    // --- Dependencies ---
    content.appendChild(h("div", { className: "section-title section-border dep-title" }, "Depends On"));
    if (obj.dependencies.length === 0) {
      content.appendChild(h("div", { className: "empty-deps" }, "No dependencies"));
    } else {
      for (const dependencyId of obj.dependencies) {
        const dependency = objectMap[dependencyId];
        content.appendChild(
          h("div", {
            className: "dep-row",
            onClick: () => drillInto(dependencyId),
          },
            h("span", { className: "dep-icon", innerHTML: dependency ? objectIcon(dependency.object_type) : ICONS.fk }),
            h("span", { className: "dep-name" }, dependency ? dependency.name : dependencyId),
            dependency ? h("span", { className: "dep-type" }, dependency.object_type) : null
          )
        );
      }
    }

    // --- Referenced By ---
    content.appendChild(h("div", { className: "section-title section-border ref-title" }, "Referenced By"));
    if (obj.dependents.length === 0) {
      content.appendChild(h("div", { className: "empty-deps" }, "No incoming references"));
    } else {
      for (const dependentId of obj.dependents) {
        const dependent = objectMap[dependentId];
        content.appendChild(
          h("div", {
            className: "ref-row",
            onClick: () => drillInto(dependentId),
          },
            h("span", { className: "ref-arrow" }, "\u2190"),
            h("span", { className: "ref-name" }, dependent ? dependent.name : dependentId),
            dependent ? h("span", { className: "ref-type" }, dependent.object_type) : null
          )
        );
      }
    }

    // --- Infrastructure ---
    if (obj.infrastructure) {
      content.appendChild(renderInfrastructure(obj.infrastructure));
    }

    root.appendChild(content);
  }

  /**
   * Renders the Infrastructure section for sources, sinks, and connections.
   * Shows connector type, connection/source references (clickable to drill
   * into), and arbitrary key-value properties (with secret/object refs
   * rendered as clickable links).
   */
  function renderInfrastructure(infra: Infrastructure): HTMLElement {
    const section = h("div", null);
    section.appendChild(h("div", { className: "section-title section-border" }, "Infrastructure"));

    if (infra.connector_type) {
      section.appendChild(
        h("div", { className: "infra-row" },
          h("span", { className: "infra-label" }, "Connector"),
          h("span", { className: "infra-value" }, infra.connector_type)
        )
      );
    }

    if (infra.connection_ref) {
      section.appendChild(
        h("div", {
          className: "infra-row clickable",
          onClick: () => drillInto(infra.connection_ref!),
        },
          h("span", { className: "infra-label" }, "Connection"),
          h("span", { className: "infra-value link" }, infra.connection_ref)
        )
      );
    }

    if (infra.source_ref) {
      section.appendChild(
        h("div", {
          className: "infra-row clickable",
          onClick: () => drillInto(infra.source_ref!),
        },
          h("span", { className: "infra-label" }, "Source"),
          h("span", { className: "infra-value link" }, infra.source_ref)
        )
      );
    }

    if (infra.properties) {
      for (const prop of infra.properties) {
        const val = prop.secret_ref || prop.object_ref || prop.value || "";
        const isLink = prop.secret_ref || prop.object_ref;

        section.appendChild(
          h("div", {
            className: "infra-row" + (isLink ? " clickable" : ""),
            onClick: isLink ? () => drillInto((prop.secret_ref || prop.object_ref)!) : null,
          },
            h("span", { className: "infra-label" }, prop.key),
            h("span", { className: "infra-value" + (isLink ? " link" : "") }, val)
          )
        );
      }
    }

    return section;
  }

  /** Toggles expansion of a database node in the browse tree and re-renders. */
  function toggleDatabase(name: string): void {
    if (expandedDatabases.has(name)) {
      expandedDatabases.delete(name);
    } else {
      expandedDatabases.add(name);
    }
    render();
  }

  /** Toggles expansion of an object's inline column preview and re-renders. */
  function toggleExpand(id: string): void {
    if (expandedObjects.has(id)) {
      expandedObjects.delete(id);
    } else {
      expandedObjects.add(id);
    }
    render();
  }

  /** Pushes an object onto the navigation stack and enters detail mode. */
  function drillInto(objectId: string): void {
    navStack.push({ id: objectId });
    hoveredColumn = null;
    render();
  }

  /** Pops the navigation stack. Returns to browse mode if stack becomes empty. */
  function goBack(): void {
    navStack.pop();
    render();
  }

  // --- Message Handling ---
  window.addEventListener("message", (event: MessageEvent) => {
    const msg = event.data as CatalogInboundMessage;
    switch (msg.type) {
      case "catalog-data":
        catalogData = msg.data;
        objectMap = {};
        for (const obj of catalogData.objects) {
          objectMap[obj.id] = obj;
        }
        // Expand all databases by default
        expandedDatabases = new Set(catalogData.databases.map((db) => db.name));
        render();
        break;

      case "inspect-object":
        if (msg.id && objectMap[msg.id]) {
          navStack = [{ id: msg.id }];
          render();
        }
        break;
    }
  });

  // Initial render + signal ready to receive data
  render();
  vscode.postMessage({ type: "ready" } satisfies CatalogOutboundMessage);
})();
