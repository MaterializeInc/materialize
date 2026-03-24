/**
 * Webview script for the mz-deploy worksheet results panel.
 *
 * Displays query results from worksheet code lens executions. The panel has
 * no SQL input — queries are initiated from `.worksheets/*.sql` files via
 * "Execute" code lenses, and results arrive as inbound messages from the
 * extension host.
 *
 * ## One-Shot Queries (SELECT, SHOW, EXPLAIN)
 *
 * Extension sends `query-result` message with columns + rows (or raw text).
 * The panel renders a single table or `<pre>` block. State: `query-result`.
 *
 * ## SUBSCRIBE (Streaming)
 *
 * ```
 * subscribe-started → mode = "subscribing", reset state, show tabs + Stop button
 *     │
 *     ├─ subscribe-batch (repeated) → apply diffs atomically:
 *     │     progress_only=true  → update timestamp display only
 *     │     progress_only=false → update Live tab (multiset) + Diffs tab (append)
 *     │
 *     └─ subscribe-complete → mode = "subscribe-done", hide Stop button
 * ```
 *
 * ### Live Tab
 *
 * Maintains the current snapshot as a multiset of rows. Each batch applies
 * diffs atomically (all changes at one timestamp together):
 * - `diff > 0` → insert N copies of the row
 * - `diff < 0` → find and remove N matching rows (value equality)
 *
 * ### Diffs Tab
 *
 * Append-only log of all diff rows with their timestamp and +/- indicator.
 * Shows the full history of changes since the SUBSCRIBE started.
 *
 * ### Progress Tracking
 *
 * SUBSCRIBE always runs WITH (PROGRESS). Progress-only batches (no data
 * changes) update the action bar timestamp (`t=<mz_timestamp>`) so the user
 * knows the system is alive even when no rows are changing. This prevents
 * the "is it stuck?" problem.
 *
 * ### Cancellation
 *
 * The "Stop" button sends a `cancel` message to the extension host, which
 * calls `mz-deploy/cancel-query`. The LSP cancels the cursor, and the
 * background task sends a `subscribe-complete` notification.
 */

declare function acquireVsCodeApi(): {
  postMessage(msg: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
};

/// <reference path="../types.ts" />

type ConnectionInfoResponse = import("../types").ConnectionInfoResponse;
type ExecuteQueryResponse = import("../types").ExecuteQueryResponse;
type WorksheetError = import("../types").WorksheetError;
type WorksheetInboundMessage = import("../types").WorksheetInboundMessage;
type WorksheetContextResponse = import("../types").WorksheetContextResponse;
type SubscribeBatch = import("../types").SubscribeBatch;

const vscode = acquireVsCodeApi();

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type PanelMode = "idle" | "query-result" | "error" | "subscribing" | "subscribe-done";

let mode: PanelMode = "idle";
let connectionInfo: ConnectionInfoResponse | null = null;
let worksheetContext: WorksheetContextResponse | null = null;

// One-shot query state
let lastResult: ExecuteQueryResponse | null = null;
let lastError: WorksheetError | null = null;

// Subscribe state
const MAX_DIFF_TIMESTAMPS = 300;
let subscribeId: string | null = null;
let subscribeColumns: string[] | null = null;
let liveRows: (string | number | boolean | null)[][] = [];
let diffLog: { timestamp: string; diff: number; values: (string | number | boolean | null)[] }[] = [];
let lastTimestamp: string | null = null;
let activeTab: "live" | "diffs" = "live";

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------

let statusDot: HTMLElement;
let statusText: HTMLElement;
let profileSelect: HTMLSelectElement;
let databaseSelect: HTMLSelectElement;
let schemaSelect: HTMLSelectElement;
let clusterSelect: HTMLSelectElement;
let actionBar: HTMLElement;
let actionInfo: HTMLElement;
let stopBtn: HTMLButtonElement;
let tabBar: HTMLElement;
let liveTab: HTMLElement;
let diffsTab: HTMLElement;
let resultsArea: HTMLElement;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function h<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  attrs?: Record<string, string>,
  ...children: (string | Node)[]
): HTMLElementTagNameMap[K] {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "className") el.className = v;
      else if (k === "textContent") el.textContent = v;
      else el.setAttribute(k, v);
    }
  }
  for (const child of children) {
    if (typeof child === "string") el.appendChild(document.createTextNode(child));
    else el.appendChild(child);
  }
  return el;
}

// ---------------------------------------------------------------------------
// Mount
// ---------------------------------------------------------------------------

function mount(): void {
  const root = document.getElementById("root")!;

  // Status bar
  const statusBarEl = h("div", { className: "status-bar" });
  statusDot = h("span", { className: "status-dot disconnected" });
  statusText = h("span", { textContent: "Connecting..." });
  statusBarEl.appendChild(statusDot);
  statusBarEl.appendChild(statusText);

  // Selector bar (Profile, Database, Schema, Cluster)
  const selectorBar = h("div", { className: "selector-bar" });

  const profileGroup = h("div", { className: "selector-group" });
  profileGroup.appendChild(h("span", { className: "selector-label", textContent: "Profile" }));
  profileSelect = h("select", { className: "selector-select", disabled: "true" }) as HTMLSelectElement;
  profileSelect.appendChild(h("option", { textContent: "—", value: "" }));
  profileGroup.appendChild(profileSelect);

  const dbGroup = h("div", { className: "selector-group" });
  dbGroup.appendChild(h("span", { className: "selector-label", textContent: "Database" }));
  databaseSelect = h("select", { className: "selector-select", disabled: "true" }) as HTMLSelectElement;
  databaseSelect.appendChild(h("option", { textContent: "—", value: "" }));
  dbGroup.appendChild(databaseSelect);

  const schemaGroup = h("div", { className: "selector-group" });
  schemaGroup.appendChild(h("span", { className: "selector-label", textContent: "Schema" }));
  schemaSelect = h("select", { className: "selector-select", disabled: "true" }) as HTMLSelectElement;
  schemaSelect.appendChild(h("option", { textContent: "—", value: "" }));
  schemaGroup.appendChild(schemaSelect);

  const clusterGroup = h("div", { className: "selector-group" });
  clusterGroup.appendChild(h("span", { className: "selector-label", textContent: "Cluster" }));
  clusterSelect = h("select", { className: "selector-select", disabled: "true" }) as HTMLSelectElement;
  clusterSelect.appendChild(h("option", { textContent: "—", value: "" }));
  clusterGroup.appendChild(clusterSelect);

  selectorBar.appendChild(profileGroup);
  selectorBar.appendChild(dbGroup);
  selectorBar.appendChild(schemaGroup);
  selectorBar.appendChild(clusterGroup);

  // Action bar
  actionBar = h("div", { className: "action-bar" });
  actionInfo = h("span", { className: "action-info" });
  stopBtn = h("button", { className: "run-btn cancel", textContent: "■ Stop" }) as HTMLButtonElement;
  stopBtn.style.display = "none";
  actionBar.appendChild(stopBtn);
  actionBar.appendChild(actionInfo);

  // Tab bar (for subscribe mode)
  tabBar = h("div", { className: "tab-bar" });
  tabBar.style.display = "none";
  liveTab = h("span", { className: "tab active", textContent: "Live" });
  diffsTab = h("span", { className: "tab", textContent: "Diffs" });
  tabBar.appendChild(liveTab);
  tabBar.appendChild(diffsTab);

  // Results area
  resultsArea = h("div", { className: "results-area" });
  resultsArea.appendChild(
    h("div", { className: "placeholder", textContent: "Execute a statement from a worksheet to see results" })
  );

  root.appendChild(statusBarEl);
  root.appendChild(selectorBar);
  root.appendChild(actionBar);
  root.appendChild(tabBar);
  root.appendChild(resultsArea);

  // Events
  profileSelect.addEventListener("change", () => {
    vscode.postMessage({ type: "set-profile", profile: profileSelect.value });
  });
  databaseSelect.addEventListener("change", () => {
    vscode.postMessage({ type: "set-session", database: databaseSelect.value });
  });
  schemaSelect.addEventListener("change", () => {
    vscode.postMessage({ type: "set-session", schema: schemaSelect.value });
  });
  clusterSelect.addEventListener("change", () => {
    vscode.postMessage({ type: "set-session", cluster: clusterSelect.value });
  });
  stopBtn.addEventListener("click", () => {
    vscode.postMessage({ type: "cancel" });
  });
  liveTab.addEventListener("click", () => {
    activeTab = "live";
    updateTabStyles();
    renderResults();
  });
  diffsTab.addEventListener("click", () => {
    activeTab = "diffs";
    updateTabStyles();
    renderResults();
  });
}

// ---------------------------------------------------------------------------
// Inbound message handler
// ---------------------------------------------------------------------------

window.addEventListener("message", (event: MessageEvent) => {
  const msg = event.data as WorksheetInboundMessage;

  switch (msg.type) {
    case "connection-info":
      connectionInfo = msg.data;
      updateConnectionStatus();
      break;

    case "query-result":
      mode = "query-result";
      lastResult = msg.data;
      lastError = null;
      subscribeId = null;
      updateUI();
      break;

    case "query-error":
      mode = "error";
      lastError = msg.error;
      lastResult = null;
      subscribeId = null;
      updateUI();
      break;

    case "worksheet-context":
      worksheetContext = msg.data;
      updateSelectors();
      break;

    case "subscribe-started":
      mode = "subscribing";
      subscribeId = msg.data.subscribe_id;
      subscribeColumns = null;
      liveRows = [];
      diffLog = [];
      lastTimestamp = null;
      activeTab = "live";
      lastResult = null;
      lastError = null;
      updateUI();
      break;

    case "subscribe-batch":
      handleSubscribeBatch(msg.data);
      break;

    case "subscribe-complete":
      mode = "subscribe-done";
      updateUI();
      break;
  }
});

// ---------------------------------------------------------------------------
// Subscribe batch processing
// ---------------------------------------------------------------------------

type DiffRow = { diff: number; values: (string | number | boolean | null)[] };
type Row = (string | number | boolean | null)[];

/**
 * Consolidate diffs within a single timestamp batch.
 *
 * Groups diffs by their serialized values and sums the diff counts. Rows
 * where the net diff is zero (e.g., a retraction and re-insertion of the
 * same values) are eliminated. This ensures that updates like
 * `-1 old_value, +1 new_value` are applied cleanly without intermediate states.
 */
function consolidateDiffs(diffs: DiffRow[]): DiffRow[] {
  const map = new Map<string, DiffRow>();
  for (const row of diffs) {
    const key = JSON.stringify(row.values);
    const existing = map.get(key);
    if (existing) {
      existing.diff += row.diff;
    } else {
      map.set(key, { diff: row.diff, values: row.values });
    }
  }
  return [...map.values()].filter((r) => r.diff !== 0);
}

/**
 * Apply consolidated diffs to the live state (multiset).
 *
 * Positive diffs insert rows, negative diffs find and remove matching rows
 * by value equality. Returns a new array (does not mutate the input).
 */
function applyDiffsToLiveState(live: Row[], consolidated: DiffRow[]): Row[] {
  const result = live.map((r) => [...r]);
  for (const row of consolidated) {
    if (row.diff > 0) {
      for (let i = 0; i < row.diff; i++) {
        result.push([...row.values]);
      }
    } else if (row.diff < 0) {
      const count = Math.abs(row.diff);
      for (let i = 0; i < count; i++) {
        const idx = result.findIndex(
          (r) =>
            r.length === row.values.length &&
            r.every((v, j) => v === row.values[j])
        );
        if (idx !== -1) result.splice(idx, 1);
      }
    }
  }
  return result;
}

/**
 * Sort rows by column values for stable visual ordering.
 *
 * Compares left to right, lexicographic on stringified values. Nulls sort
 * before non-nulls. This prevents rows from jumping around the screen as
 * diffs arrive — new rows slot into their sorted position.
 */
function sortRows(rows: Row[]): Row[] {
  return rows.sort((a, b) => {
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
      const av = a[i] === null ? "" : String(a[i]);
      const bv = b[i] === null ? "" : String(b[i]);
      if (av < bv) return -1;
      if (av > bv) return 1;
    }
    return a.length - b.length;
  });
}

function handleSubscribeBatch(batch: SubscribeBatch): void {
  if (batch.subscribe_id !== subscribeId) return;

  lastTimestamp = batch.timestamp;

  // Evict diff entries older than the timestamp window.
  const cutoff = BigInt(batch.timestamp) - BigInt(MAX_DIFF_TIMESTAMPS);
  diffLog = diffLog.filter((entry) => BigInt(entry.timestamp) >= cutoff);

  if (batch.columns) {
    subscribeColumns = batch.columns;
  }

  if (!batch.progress_only) {
    // Append raw diffs to the changelog (for the Diffs tab).
    for (const row of batch.diffs) {
      diffLog.push({
        timestamp: batch.timestamp,
        diff: row.diff,
        values: row.values,
      });
    }

    // Consolidate and apply atomically to the live state.
    const consolidated = consolidateDiffs(batch.diffs);
    liveRows = sortRows(applyDiffsToLiveState(liveRows, consolidated));
  }

  updateUI();

  // Flash the timestamp to signal liveness.
  const tsEl = actionInfo.querySelector(".timestamp-value");
  if (tsEl) {
    tsEl.classList.remove("flash");
    // Force reflow so removing + re-adding the class triggers the animation.
    void (tsEl as HTMLElement).offsetWidth;
    tsEl.classList.add("flash");
  }
}

// ---------------------------------------------------------------------------
// UI rendering
// ---------------------------------------------------------------------------

function updateConnectionStatus(): void {
  if (!connectionInfo) return;
  if (connectionInfo.connected) {
    statusDot.className = "status-dot connected";
    const user = connectionInfo.user ? `${connectionInfo.user}@` : "";
    statusText.textContent = `${user}${connectionInfo.host}:${connectionInfo.port} (${connectionInfo.profile})`;
  } else {
    statusDot.className = "status-dot disconnected";
    statusText.textContent = connectionInfo.message || "Not connected";
  }
}

function updateSelectors(): void {
  if (!worksheetContext) return;
  const ctx = worksheetContext;
  populateSelect(profileSelect, ctx.profiles, ctx.current_profile);
  const databases = Object.keys(ctx.database_schemas).sort();
  populateSelect(databaseSelect, databases, ctx.current_database);
  const currentDb = databaseSelect.value;
  const schemas = currentDb ? (ctx.database_schemas[currentDb] || []) : [];
  populateSelect(schemaSelect, schemas, ctx.current_schema);
  populateSelect(clusterSelect, ctx.clusters, ctx.current_cluster);
}

function populateSelect(
  select: HTMLSelectElement,
  items: string[],
  current: string | undefined,
): void {
  const prevValue = select.value;
  select.innerHTML = "";
  if (items.length === 0) {
    select.appendChild(h("option", { textContent: "—", value: "" }));
    select.disabled = true;
    return;
  }
  for (const item of items) {
    select.appendChild(h("option", { textContent: item, value: item }));
  }
  select.disabled = false;
  if (current && items.includes(current)) {
    select.value = current;
  } else if (prevValue && items.includes(prevValue)) {
    select.value = prevValue;
  }
}

function updateTabStyles(): void {
  liveTab.className = activeTab === "live" ? "tab active" : "tab";
  diffsTab.className = activeTab === "diffs" ? "tab active" : "tab";
}

function updateStopButton(): void {
  if (mode === "subscribing") {
    stopBtn.style.display = "inline-block";
    stopBtn.disabled = false;
    stopBtn.className = "run-btn cancel";
  } else if (mode === "subscribe-done") {
    stopBtn.style.display = "inline-block";
    stopBtn.disabled = true;
    stopBtn.className = "run-btn cancel disabled";
  } else {
    stopBtn.style.display = "none";
  }
}

function updateActionInfo(): void {
  if (mode === "subscribing" || mode === "subscribe-done") {
    const label = mode === "subscribing" ? "Subscribing" : "Stopped";
    const rowCount = liveRows.length;
    actionInfo.innerHTML = "";
    actionInfo.className = "action-info";
    actionInfo.appendChild(document.createTextNode(
      `${label} · ${rowCount} row${rowCount !== 1 ? "s" : ""}`
    ));
    if (lastTimestamp) {
      actionInfo.appendChild(document.createTextNode(" · "));
      const tsSpan = h("span", { className: "timestamp-value", textContent: `t=${lastTimestamp}` });
      actionInfo.appendChild(tsSpan);
    }
  } else if (mode === "query-result" && lastResult) {
    const rows = lastResult.rows ? lastResult.rows.length : 0;
    const truncLabel = lastResult.truncated ? " (truncated)" : "";
    actionInfo.textContent = `${lastResult.elapsed_ms}ms · ${rows} row${rows !== 1 ? "s" : ""}${truncLabel}`;
    actionInfo.className = lastResult.truncated ? "action-info truncated" : "action-info";
  } else {
    actionInfo.textContent = "";
    actionInfo.className = "action-info";
  }
}

function updateUI(): void {
  const isSubscribe = mode === "subscribing" || mode === "subscribe-done";
  tabBar.style.display = isSubscribe ? "flex" : "none";
  updateStopButton();
  updateTabStyles();
  updateActionInfo();
  renderResults();
}

function renderError(): void {
  const err = lastError;
  if (err) {
    const card = h("div", { className: "error-card" });
    card.appendChild(h("div", { className: "error-code", textContent: err.code }));
    card.appendChild(h("div", { className: "error-message", textContent: err.message }));
    if (err.hint) {
      card.appendChild(h("div", { className: "error-hint", textContent: err.hint }));
    }
    resultsArea.appendChild(card);
  }
}

function renderQueryResult(): void {
  if (lastResult!.raw_text !== undefined && lastResult!.raw_text !== null) {
    resultsArea.appendChild(
      h("pre", { className: "raw-text", textContent: lastResult!.raw_text })
    );
    return;
  }
  if (lastResult!.columns && lastResult!.rows) {
    resultsArea.appendChild(buildTable(lastResult!.columns, lastResult!.rows));
    return;
  }
  resultsArea.appendChild(
    h("div", { className: "placeholder", textContent: "Query returned no rows" })
  );
}

function renderSubscribeLive(): void {
  if (!subscribeColumns || liveRows.length === 0) {
    resultsArea.appendChild(
      h("div", { className: "placeholder", textContent: mode === "subscribing" ? "Waiting for data..." : "No rows" })
    );
    return;
  }
  resultsArea.appendChild(buildTable(subscribeColumns, liveRows));
}

function renderSubscribeDiffs(): void {
  if (diffLog.length === 0) {
    resultsArea.appendChild(
      h("div", { className: "placeholder", textContent: mode === "subscribing" ? "Waiting for data..." : "No diffs" })
    );
    return;
  }
  const cols = ["timestamp", "diff", ...(subscribeColumns || [])];
  const rows = diffLog.map((d) => [
    d.timestamp,
    d.diff > 0 ? `+${d.diff}` : `${d.diff}`,
    ...d.values,
  ]);
  resultsArea.appendChild(buildTable(cols, rows));
}

function renderIdle(): void {
  resultsArea.appendChild(
    h("div", { className: "placeholder", textContent: "Execute a statement from a worksheet to see results" })
  );
}

function renderResults(): void {
  resultsArea.innerHTML = "";
  if (mode === "error" || (mode === "subscribe-done" && lastError)) { renderError(); return; }
  if (mode === "query-result" && lastResult) { renderQueryResult(); return; }
  if ((mode === "subscribing" || mode === "subscribe-done") && activeTab === "live") { renderSubscribeLive(); return; }
  if ((mode === "subscribing" || mode === "subscribe-done") && activeTab === "diffs") { renderSubscribeDiffs(); return; }
  renderIdle();
}

function buildTable(
  columns: string[],
  rows: (string | number | boolean | null)[][],
): HTMLTableElement {
  const table = h("table", { className: "results-table" });
  const thead = h("thead");
  const headerRow = h("tr");
  for (const col of columns) {
    headerRow.appendChild(h("th", { textContent: col }));
  }
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = h("tbody");
  for (const row of rows) {
    const tr = h("tr");
    for (const cell of row) {
      if (cell === null) {
        tr.appendChild(h("td", { className: "null-value", textContent: "NULL" }));
      } else {
        tr.appendChild(h("td", { textContent: String(cell) }));
      }
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  return table;
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

mount();
vscode.postMessage({ type: "ready" });
