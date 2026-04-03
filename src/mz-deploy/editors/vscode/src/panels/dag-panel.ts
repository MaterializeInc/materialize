/**
 * Singleton WebviewPanel manager for the schema DAG view.
 *
 * Handles panel creation, data updates, focus changes, and bidirectional
 * navigation with the catalog sidebar. Only one DAG panel exists at a time;
 * calling open() on an existing panel reveals it and updates the focus.
 */

import * as vscode from "vscode";
import * as crypto from "crypto";
import { DagData, DagOutboundMessage } from "../types";

export class DAGPanel {
  private _extensionUri: vscode.Uri;
  private _panel: vscode.WebviewPanel | null;
  private _dagData: DagData | null;
  private _onMessage: ((msg: DagOutboundMessage) => void) | null;

  constructor(extensionUri: vscode.Uri) {
    this._extensionUri = extensionUri;
    this._panel = null;
    this._dagData = null;
    this._onMessage = null;
  }

  /**
   * Set a callback for messages from the webview.
   * Messages: { type: 'inspect-object', id }
   */
  onMessage(callback: (msg: DagOutboundMessage) => void): void {
    this._onMessage = callback;
  }

  /**
   * Push DAG data to the panel (if open).
   */
  setDAGData(data: DagData): void {
    this._dagData = data;
    if (this._panel) {
      void this._panel.webview.postMessage({ type: "dag-data", data });
    }
  }

  /**
   * Open the DAG panel (or reveal if already open), optionally focused on a table.
   */
  open(focusTable: string | null): void {
    if (this._panel) {
      this._panel.reveal();
      if (focusTable) {
        void this._panel.webview.postMessage({ type: "focus", id: focusTable });
      }
      return;
    }

    this._panel = vscode.window.createWebviewPanel(
      "mzDeployDAG",
      "DAG View",
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode.Uri.joinPath(this._extensionUri, "panels"),
          vscode.Uri.joinPath(this._extensionUri, "out", "webview", "panels"),
        ],
      }
    );

    this._panel.webview.html = this._getHtml(this._panel.webview);

    this._panel.webview.onDidReceiveMessage((msg: DagOutboundMessage) => {
      if (this._onMessage) {
        this._onMessage(msg);
      }
    });

    // Send data once webview is ready
    this._panel.webview.onDidReceiveMessage((msg: DagOutboundMessage) => {
      if (msg.type === "ready") {
        if (this._dagData) {
          void this._panel!.webview.postMessage({ type: "dag-data", data: this._dagData });
        }
        if (focusTable) {
          void this._panel!.webview.postMessage({ type: "focus", id: focusTable });
        }
      }
    });

    this._panel.onDidDispose(() => {
      this._panel = null;
    });
  }

  private _getHtml(webview: vscode.Webview): string {
    const nonce = crypto.randomBytes(16).toString("hex");
    const cssUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._extensionUri, "panels", "dag.css")
    );
    const jsUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._extensionUri, "out", "webview", "panels", "dag.js")
    );

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}';">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="${cssUri}">
</head>
<body>
  <div id="dag-root">
    <div id="dag-header"></div>
    <div id="dag-canvas"></div>
    <div id="dag-legend"></div>
  </div>
  <script nonce="${nonce}" src="${jsUri}"></script>
</body>
</html>`;
  }
}
