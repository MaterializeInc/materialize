/**
 * VS Code extension entry point for mz-deploy.
 *
 * Orchestrates four subsystems on top of the mz-deploy LSP server:
 *
 * 1. **Data Catalog sidebar** — Tree-browsable object catalog with drill-down
 *    detail views, powered by `mz-deploy/catalog` LSP requests.
 * 2. **DAG panel** — Layered dependency graph visualization, powered by
 *    `mz-deploy/dag` LSP requests.
 * 3. **Keyword highlighting** — Context-aware SQL keyword decoration using
 *    keywords fetched from `mz-deploy/keywords`.
 * 4. **SQL Worksheet** — Files in `worksheets/` get "Execute" code lenses.
 *    Results display in a bottom panel. Supports two execution modes:
 *    - **One-shot** (SELECT, SHOW, EXPLAIN): request-response via
 *      `mz-deploy/execute-query`, results rendered as table or raw text.
 *    - **SUBSCRIBE**: streaming via `mz-deploy/subscribe` request +
 *      `mz-deploy/subscribeBatch` notifications. The panel shows two tabs:
 *      "Live" (current snapshot by applying diffs) and "Diffs" (append-only
 *      changelog). Cancelled via `mz-deploy/cancel-query`.
 *
 * ## Data Flow
 *
 *     LSP Server (mz-deploy lsp)
 *         │
 *         ▼
 *     LanguageClient ──► mz-deploy/catalog ──► CatalogProvider ──► catalog.js
 *                    ──► mz-deploy/dag     ──► DAGPanel        ──► dag.js
 *                    ──► mz-deploy/keywords ──► applyKeywordDecorations()
 *                    ──► mz-deploy/execute-query ──► WorksheetProvider ──► worksheet.js
 *
 *     SUBSCRIBE flow (streaming):
 *     LanguageClient ──► mz-deploy/subscribe (request) ──► returns subscribe_id
 *                    ◄── mz-deploy/subscribeBatch (notification, repeated)
 *                    ◄── mz-deploy/subscribeComplete (notification, once)
 *                    ──► WorksheetProvider ──► worksheet.js (Live + Diffs tabs)
 *
 * ## Refresh Lifecycle
 *
 * On file save the LSP server rebuilds the project and emits a
 * `mz-deploy/projectRebuilt` notification. The extension responds by
 * re-requesting catalog and DAG data, keeping the UI in sync without polling.
 *
 * ## Keyword Highlighting Rules
 *
 * SQL keywords are decorated with `mzDeploy.keywordForeground`. A match is
 * suppressed when it falls inside:
 * - A single-quoted string literal (with `''` escape handling)
 * - A double-quoted identifier
 * - A `--` line comment
 * - A parenthesized column list following `ON` or `REFERENCES`
 * - The token immediately after an identifier-context word (e.g., `VIEW`,
 *   `TABLE`, `SOURCE`) where the next word is a user-defined name
 * - A dot-qualified name (`schema.keyword` or `keyword.col`)
 */

import { LanguageClient, ServerOptions, LanguageClientOptions } from "vscode-languageclient/node";
import * as vscode from "vscode";
import * as path from "path";
import * as os from "os";
import { CatalogProvider } from "./sidebar/catalog-provider";
import { DAGPanel } from "./panels/dag-panel";
import { DagData, CatalogData, CatalogOutboundMessage, DagOutboundMessage } from "./types";
import { WorksheetProvider } from "./sidebar/worksheet-provider";
import { WorksheetOutboundMessage, ConnectionInfoResponse, ExecuteQueryResponse, WorksheetError, WorksheetContextResponse, SubscribeStarted, SubscribeBatch, SubscribeComplete } from "./types";

let client: LanguageClient;
let catalogProvider: CatalogProvider | null = null;
let dagPanel: DAGPanel | null = null;
let keywordDecorationType: vscode.TextEditorDecorationType | null = null;
let keywordRegex: RegExp | null = null;
let worksheetProvider: WorksheetProvider | null = null;

/** Extracts a structured WorksheetError from an RPC error, falling back to a generic internal error. */
function extractRpcError(err: unknown): WorksheetError {
  const rpcErr = err as { data?: WorksheetError; message?: string };
  return rpcErr.data || {
    code: "internal_error",
    message: rpcErr.message || "Unknown error",
  };
}

/** Returns the filesystem path of the first open workspace folder, or undefined. */
function getWorkspacePath(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

/** Fetches the dependency graph from the LSP server and pushes it to the DAG panel. */
async function requestDagData(): Promise<void> {
  if (!client || !client.isRunning()) return;
  try {
    const data = await client.sendRequest<DagData>("mz-deploy/dag");
    if (data && dagPanel) dagPanel.setDAGData(data);
  } catch (err) {
    console.error("[mz-deploy] dag request failed:", err);
  }
}

/** Fetches the catalog from the LSP server and pushes it to the sidebar provider. */
async function requestCatalogData(): Promise<void> {
  if (!client || !client.isRunning()) return;
  try {
    const data = await client.sendRequest<CatalogData>("mz-deploy/catalog");
    if (data && catalogProvider) {
      catalogProvider.setCatalogData(data);
    }
  } catch (err) {
    console.error("[mz-deploy] catalog request failed:", err);
  }
}

/** Fetches connection info from the LSP server and pushes it to the worksheet panel. */
async function requestConnectionInfo(): Promise<void> {
  if (!client || !client.isRunning()) return;
  try {
    const data = await client.sendRequest<ConnectionInfoResponse>("mz-deploy/connection-info");
    if (data && worksheetProvider) {
      worksheetProvider.postMessage({ type: "connection-info", data });
    }
  } catch (err) {
    console.error("[mz-deploy] connection-info request failed:", err);
  }
}

/** Fetches worksheet context (databases, schemas, clusters) and pushes it to the worksheet panel. */
async function requestWorksheetContext(): Promise<void> {
  console.log("[mz-deploy] requestWorksheetContext called, client running:", client?.isRunning());
  if (!client || !client.isRunning()) return;
  try {
    const data = await client.sendRequest<WorksheetContextResponse>("mz-deploy/worksheet-context");
    console.log("[mz-deploy] worksheet-context response:", JSON.stringify(data).slice(0, 200));
    if (data && worksheetProvider) {
      worksheetProvider.postMessage({ type: "worksheet-context", data });
    }
  } catch (err) {
    console.error("[mz-deploy] worksheet-context request failed:", err);
  }
}

/**
 * Fetches the keyword list from the LSP server and initializes the decoration
 * type used for SQL keyword highlighting. Called once after client start.
 *
 * Builds a single regex from all keywords (escaped for regex safety), then
 * applies decorations to the active editor. Subsequent updates are driven by
 * `onDidChangeActiveTextEditor` and `onDidChangeTextDocument` listeners.
 */
async function initKeywordHighlighting(): Promise<void> {
  if (!client || !client.isRunning()) return;
  try {
    const keywords = await client.sendRequest<string[]>("mz-deploy/keywords");
    if (!keywords || !keywords.length) return;

    const pattern = keywords
      .map((k: string) => k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join("|");
    keywordRegex = new RegExp(`\\b(${pattern})\\b`, "gi");

    keywordDecorationType = vscode.window.createTextEditorDecorationType({
      color: new vscode.ThemeColor("mzDeploy.keywordForeground"),
    });

    if (vscode.window.activeTextEditor) {
      applyKeywordDecorations(vscode.window.activeTextEditor);
    }
  } catch (err) {
    console.error("[mz-deploy] keywords request failed:", err);
  }
}

/**
 * SQL words after which the next token is a user-defined name, not a keyword.
 *
 * For example, in `CREATE VIEW my_view`, the word `my_view` follows `VIEW`
 * and should not be highlighted even if it happens to match a keyword.
 */
const IDENTIFIER_CONTEXT_WORDS: readonly string[] = [
  "CLUSTER", "AS", "VIEW", "TABLE", "SOURCE", "SINK", "INDEX",
  "SECRET", "CONNECTION", "TYPE", "SCHEMA", "DATABASE", "ROLE", "TEST",
] as const;

/**
 * Scans `text` and returns an array of `[start, end)` byte ranges that should
 * be excluded from keyword highlighting.
 *
 * Excluded regions:
 * - Single-quoted string literals (handles `''` escapes)
 * - Double-quoted identifiers
 * - `--` line comments
 * - Parenthesized column lists following `ON` or `REFERENCES` (e.g., index and
 *   FK definitions), detected via {@link isColumnListParen}
 *
 * The returned ranges are sorted by start offset, which allows
 * {@link isInsideRange} to short-circuit with an early break.
 */
/** Scans a single-quoted string literal starting at `start`, handling `''` escapes. */
function scanSingleQuoted(text: string, start: number): [number, [number, number]] {
  let i = start + 1;
  while (i < text.length) {
    if (text[i] === "'" && text[i + 1] === "'") {
      i += 2;
    } else if (text[i] === "'") {
      i++;
      break;
    } else {
      i++;
    }
  }
  return [i, [start, i]];
}

/** Scans a double-quoted identifier starting at `start`. */
function scanDoubleQuoted(text: string, start: number): [number, [number, number]] {
  let i = start + 1;
  while (i < text.length && text[i] !== '"') i++;
  if (i < text.length) i++;
  return [i, [start, i]];
}

/** Scans a `--` line comment starting at `start`. */
function scanLineComment(text: string, start: number): [number, [number, number]] {
  let i = start + 2;
  while (i < text.length && text[i] !== "\n") i++;
  return [i, [start, i]];
}

/** Scans a parenthesized column list starting at `start`, returning null range if not a column list paren. */
function scanColumnListParen(text: string, start: number): [number, [number, number] | null] {
  if (isColumnListParen(text, start)) {
    let depth = 1;
    let i = start + 1;
    while (i < text.length && depth > 0) {
      if (text[i] === "(") depth++;
      else if (text[i] === ")") depth--;
      i++;
    }
    return [i, [start, i]];
  } else {
    return [start + 1, null];
  }
}

function buildExcludedRanges(text: string): [number, number][] {
  const ranges: [number, number][] = [];
  let i = 0;
  while (i < text.length) {
    if (text[i] === "'") {
      const [next, range] = scanSingleQuoted(text, i);
      ranges.push(range); i = next;
    } else if (text[i] === '"') {
      const [next, range] = scanDoubleQuoted(text, i);
      ranges.push(range); i = next;
    } else if (text[i] === "-" && text[i + 1] === "-") {
      const [next, range] = scanLineComment(text, i);
      ranges.push(range); i = next;
    } else if (text[i] === "(") {
      const [next, range] = scanColumnListParen(text, i);
      if (range) ranges.push(range);
      i = next;
    } else {
      i++;
    }
  }
  return ranges;
}

/**
 * Returns true if the opening paren at `parenOffset` begins a column list in
 * an `ON (...)` or `REFERENCES (...)` clause.
 *
 * Walks backward past optional whitespace and a preceding identifier to find
 * the keyword. Column names inside these parens should not be highlighted.
 */
function isColumnListParen(text: string, parenOffset: number): boolean {
  let i = parenOffset - 1;
  while (i >= 0 && /\s/.test(text[i])) i--;
  if (i < 0) return false;
  while (i >= 0 && /[\w.]/.test(text[i])) i--;
  while (i >= 0 && /\s/.test(text[i])) i--;
  if (i < 0) return false;
  const wordEnd = i + 1;
  while (i >= 0 && /\w/.test(text[i])) i--;
  const word = text.substring(i + 1, wordEnd).toUpperCase();
  return word === "ON" || word === "REFERENCES";
}

/**
 * Returns true if the word immediately before `offset` (skipping whitespace)
 * is an identifier-context word, meaning the token at `offset` is a
 * user-defined name and should not be highlighted as a keyword.
 */
function isPrecededByIdentifierContext(text: string, offset: number): boolean {
  let i = offset - 1;
  while (i >= 0 && /\s/.test(text[i])) i--;
  if (i < 0) return false;
  const wordEnd = i + 1;
  while (i >= 0 && /\w/.test(text[i])) i--;
  const word = text.substring(i + 1, wordEnd).toUpperCase();
  return IDENTIFIER_CONTEXT_WORDS.includes(word);
}

/**
 * Returns true if `offset` falls within any of the sorted `[start, end)` ranges.
 * Short-circuits once ranges start past the offset.
 */
function isInsideRange(offset: number, ranges: [number, number][]): boolean {
  for (const [start, end] of ranges) {
    if (offset >= start && offset < end) return true;
    if (start > offset) break;
  }
  return false;
}

/**
 * Computes and applies keyword decorations to the given editor.
 *
 * For each regex match against the document text, the match is suppressed if
 * it falls inside an excluded range, is preceded by an identifier-context
 * word, or is part of a dot-qualified name. Only `.sql` files are decorated.
 */
function applyKeywordDecorations(editor: vscode.TextEditor): void {
  if (!keywordRegex || !keywordDecorationType) return;
  if (editor.document.languageId !== "sql") return;

  const text = editor.document.getText();
  const excluded = buildExcludedRanges(text);
  const decorations: vscode.DecorationOptions[] = [];
  let match: RegExpExecArray | null;
  keywordRegex.lastIndex = 0;
  while ((match = keywordRegex.exec(text)) !== null) {
    if (isInsideRange(match.index, excluded)) continue;
    if (isPrecededByIdentifierContext(text, match.index)) continue;
    const before = match.index > 0 ? text[match.index - 1] : "";
    const after = text[match.index + match[0].length] || "";
    if (before === "." || after === ".") continue;
    const startPos = editor.document.positionAt(match.index);
    const endPos = editor.document.positionAt(match.index + match[0].length);
    decorations.push({ range: new vscode.Range(startPos, endPos) });
  }
  editor.setDecorations(keywordDecorationType, decorations);
}

/**
 * Extension activation entry point. Called by VS Code when a workspace
 * containing `project.toml` is opened.
 *
 * Sets up:
 * 1. The LSP client pointing at `mz-deploy lsp`
 * 2. The catalog sidebar (`CatalogProvider`) and DAG panel (`DAGPanel`)
 * 3. Message routing between webviews and the extension host
 * 4. Commands: `mz-deploy.openDAG`, `mz-deploy.runTest`
 * 5. The `mz-deploy/projectRebuilt` notification handler for live refresh
 * 6. Keyword highlighting listeners on editor/document changes
 */
/** Handles messages from the worksheet webview, routing them to the LSP server. */
async function handleWorksheetMessage(msg: WorksheetOutboundMessage): Promise<void> {
  console.log("[mz-deploy] worksheet message:", msg.type);
  if (!client || !client.isRunning()) {
    console.warn("[mz-deploy] worksheet: LSP client not running, dropping message:", msg.type);
    return;
  }

  switch (msg.type) {
    case "ready":
      void requestConnectionInfo();
      void requestWorksheetContext();
      break;

    case "execute":
      try {
        console.log("[mz-deploy] worksheet: sending execute-query for:", msg.query);
        const data = await client.sendRequest<ExecuteQueryResponse>(
          "mz-deploy/execute-query",
          { query: msg.query, timeout_ms: msg.timeout_ms }
        );
        console.log("[mz-deploy] worksheet: got result, rows:", data?.rows?.length);
        worksheetProvider!.postMessage({ type: "query-result", data });
      } catch (err: unknown) {
        console.error("[mz-deploy] worksheet: execute-query error:", err);
        worksheetProvider!.postMessage({ type: "query-error", error: extractRpcError(err) });
      }
      break;

    case "cancel":
      try {
        await client.sendRequest("mz-deploy/cancel-query");
      } catch (err) {
        console.error("[mz-deploy] cancel-query failed:", err);
      }
      break;

    case "request-connection-info":
      void requestConnectionInfo();
      break;

    case "request-worksheet-context":
      void requestWorksheetContext();
      break;

    case "set-session":
      try {
        const ctx = await client.sendRequest<WorksheetContextResponse>(
          "mz-deploy/set-session",
          { database: msg.database, schema: msg.schema, cluster: msg.cluster }
        );
        worksheetProvider!.postMessage({ type: "worksheet-context", data: ctx });
      } catch (err) {
        console.error("[mz-deploy] set-session failed:", err);
      }
      break;

    case "set-profile":
      try {
        const ctx = await client.sendRequest<WorksheetContextResponse>(
          "mz-deploy/set-profile",
          { profile: msg.profile }
        );
        worksheetProvider!.postMessage({ type: "worksheet-context", data: ctx });
        void requestConnectionInfo();
      } catch (err) {
        console.error("[mz-deploy] set-profile failed:", err);
      }
      break;
  }
}

/** Registers all VS Code commands and pushes their disposables to the extension context. */
function registerCommands(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.openDAG", () => {
      dagPanel!.open(null);
      void requestDagData();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runTest", async (filter: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      const terminal = vscode.window.createTerminal("mz-deploy test");
      terminal.show();
      terminal.sendText(
        `~/materialize/target/release/mz-deploy test '${filter}'`
      );
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runExplain", async (target: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      const terminal = vscode.window.createTerminal("mz-deploy explain");
      terminal.show();
      terminal.sendText(
        `~/materialize/target/release/mz-deploy explain '${target}'`
      );
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.executeStatement", async (sql: string) => {
      if (!client || !client.isRunning()) return;
      // Ensure the worksheet results panel is visible.
      await vscode.commands.executeCommand("mz-deploy-worksheet.focus");

      // Route SUBSCRIBE to the streaming endpoint.
      const isSubscribe = sql.trim().toUpperCase().startsWith("SUBSCRIBE");
      if (isSubscribe) {
        try {
          const data = await client.sendRequest<SubscribeStarted>(
            "mz-deploy/subscribe",
            { query: sql }
          );
          worksheetProvider!.postMessage({ type: "subscribe-started", data });
        } catch (err: unknown) {
          worksheetProvider!.postMessage({ type: "query-error", error: extractRpcError(err) });
        }
        return;
      }

      // Regular one-shot query (SELECT, SHOW, EXPLAIN, or DML).
      try {
        const data = await client.sendRequest<ExecuteQueryResponse>(
          "mz-deploy/execute-query",
          { query: sql, timeout_ms: 30000 }
        );
        // DML response — show notification, don't touch results panel.
        if (data.affected_rows !== undefined) {
          const n = data.affected_rows;
          void vscode.window.showInformationMessage(
            `${n} row${n !== 1 ? "s" : ""} affected (${data.elapsed_ms}ms)`
          );
          return;
        }
        worksheetProvider!.postMessage({ type: "query-result", data });
      } catch (err: unknown) {
        worksheetProvider!.postMessage({ type: "query-error", error: extractRpcError(err) });
      }
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.openWorksheet", async () => {
      const workspace = getWorkspacePath();
      if (!workspace) return;
      const dir = path.join(workspace, "worksheets");
      await vscode.workspace.fs.createDirectory(vscode.Uri.file(dir));
      const name = `worksheet-${Date.now()}.sql`;
      const filePath = path.join(dir, name);
      await vscode.workspace.fs.writeFile(
        vscode.Uri.file(filePath),
        Buffer.from("-- Materialize Worksheet\n\n")
      );
      const doc = await vscode.workspace.openTextDocument(filePath);
      await vscode.window.showTextDocument(doc);
    })
  );
}

/** Registers LSP notification handlers for project rebuilds and SUBSCRIBE streaming. */
function registerNotificationHandlers(): void {
  // Refresh catalog and DAG data when the LSP server finishes rebuilding
  // the project (triggered by file saves). Registered before start() so the
  // handler is in place when the first notification arrives.
  client.onNotification("mz-deploy/projectRebuilt", () => {
    void requestCatalogData();
    void requestDagData();
    void requestConnectionInfo();
  });

  // --- SUBSCRIBE notifications ---
  client.onNotification("mz-deploy/subscribeBatch", (batch: SubscribeBatch) => {
    if (worksheetProvider) {
      worksheetProvider.postMessage({ type: "subscribe-batch", data: batch });
    }
  });
  client.onNotification("mz-deploy/subscribeComplete", (data: SubscribeComplete) => {
    if (worksheetProvider) {
      worksheetProvider.postMessage({ type: "subscribe-complete", data });
    }
  });
}

export function activate(context: vscode.ExtensionContext): void {
  const command = `${os.homedir()}/materialize/target/release/mz-deploy`;
  const workspaceFolder = getWorkspacePath();

  const serverOptions: ServerOptions = {
    run: { command, args: ["lsp", "-d", workspaceFolder || "."] },
    debug: { command, args: ["lsp", "-d", workspaceFolder || "."] },
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "sql" }],
  };

  client = new LanguageClient(
    "mz-deploy-lsp",
    "mz-deploy LSP",
    serverOptions,
    clientOptions
  );

  // --- Sidebar: Data Catalog ---
  catalogProvider = new CatalogProvider(context.extensionUri);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider("mz-deploy-catalog", catalogProvider)
  );

  // --- Editor Panels ---
  dagPanel = new DAGPanel(context.extensionUri);
  // --- Sidebar message routing ---
  catalogProvider.onMessage((msg: CatalogOutboundMessage) => {
    switch (msg.type) {
      case "open-file": {
        const workspace = getWorkspacePath();
        if (workspace && msg.path) {
          const absPath = path.join(workspace, msg.path);
          void vscode.workspace.openTextDocument(absPath).then((doc) => {
            void vscode.window.showTextDocument(doc, { viewColumn: vscode.ViewColumn.One });
          });
        }
        break;
      }
      case "open-dag":
        dagPanel!.open(msg.focusTable || null);
        void requestDagData();
        break;
    }
  });

  // --- DAG panel message routing ---
  dagPanel.onMessage((msg: DagOutboundMessage) => {
    if (msg.type === "inspect-object" && msg.id) {
      catalogProvider!.inspectObject(msg.id);
    }
  });

  // --- Bottom Panel: Worksheet ---
  worksheetProvider = new WorksheetProvider(context.extensionUri);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider("mz-deploy-worksheet", worksheetProvider)
  );
  worksheetProvider.onMessage(handleWorksheetMessage);

  // --- Commands ---
  registerCommands(context);

  // --- Notification handlers ---
  registerNotificationHandlers();

  // --- LSP startup ---
  void client.start().then(async () => {
    await initKeywordHighlighting();
    void requestCatalogData();
    void requestDagData();
    void requestConnectionInfo();
    void requestWorksheetContext();
  });

  // --- Editor listeners for keyword highlighting ---
  vscode.window.onDidChangeActiveTextEditor(
    (editor: vscode.TextEditor | undefined) => {
      if (editor) applyKeywordDecorations(editor);
    },
    null,
    context.subscriptions
  );

  vscode.workspace.onDidChangeTextDocument(
    (event: vscode.TextDocumentChangeEvent) => {
      const editor = vscode.window.activeTextEditor;
      if (editor && event.document === editor.document) {
        applyKeywordDecorations(editor);
      }
    },
    null,
    context.subscriptions
  );
}

/** Extension deactivation. Stops the LSP client. */
export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
