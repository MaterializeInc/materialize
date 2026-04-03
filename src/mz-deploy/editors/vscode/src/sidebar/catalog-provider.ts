/**
 * WebviewViewProvider for the mz-deploy data catalog sidebar.
 *
 * Manages the webview lifecycle, CSP, and message routing between the catalog
 * webview and the extension host. The extension host pushes catalog data down
 * via setCatalogData(); the webview sends navigation events (open-file,
 * open-dag, inspect-object) back up for the extension host to route.
 */

import * as vscode from "vscode";
import * as crypto from "crypto";
import { CatalogData, CatalogOutboundMessage } from "../types";

export class CatalogProvider implements vscode.WebviewViewProvider {
  private _extensionUri: vscode.Uri;
  private _view: vscode.WebviewView | null;
  private _catalogData: CatalogData | null;
  private _onMessage: ((msg: CatalogOutboundMessage) => void) | null; // callback set by extension host

  constructor(extensionUri: vscode.Uri) {
    this._extensionUri = extensionUri;
    this._view = null;
    this._catalogData = null;
    this._onMessage = null;
  }

  /**
   * Called by VS Code when the sidebar view becomes visible.
   */
  resolveWebviewView(
    webviewView: vscode.WebviewView,
    _context: vscode.WebviewViewResolveContext,
    _token: vscode.CancellationToken
  ): void {
    this._view = webviewView;

    webviewView.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(this._extensionUri, "sidebar"),
        vscode.Uri.joinPath(this._extensionUri, "out", "webview", "sidebar"),
      ],
    };

    webviewView.webview.html = this._getHtml(webviewView.webview);

    // When the sidebar becomes visible again, push any cached data that
    // arrived while it was hidden (setCatalogData skips hidden views).
    webviewView.onDidChangeVisibility(() => {
      if (webviewView.visible && this._catalogData) {
        void webviewView.webview.postMessage({
          type: "catalog-data",
          data: this._catalogData,
        });
      }
    });

    // Forward messages from webview to extension host
    webviewView.webview.onDidReceiveMessage((msg: CatalogOutboundMessage) => {
      // When the webview script loads, it signals ready — send cached data
      if (msg.type === "ready") {
        if (this._catalogData) {
          void webviewView.webview.postMessage({
            type: "catalog-data",
            data: this._catalogData,
          });
        }
        return;
      }
      if (this._onMessage) {
        this._onMessage(msg);
      }
    });
  }

  /**
   * Set a callback for messages from the webview.
   * Messages: { type: 'open-file', path }, { type: 'open-dag', focusTable }
   */
  onMessage(callback: (msg: CatalogOutboundMessage) => void): void {
    this._onMessage = callback;
  }

  /**
   * Push catalog data to the webview.
   * @param {object} data - CatalogResponse from the LSP (databases[], objects[])
   */
  setCatalogData(data: CatalogData): void {
    this._catalogData = data;
    if (this._view && this._view.visible) {
      void this._view.webview.postMessage({
        type: "catalog-data",
        data,
      });
    }
  }

  /**
   * Navigate the sidebar to an object's detail view.
   * Called by the extension host when e.g. a DAG node is clicked.
   */
  inspectObject(objectId: string): void {
    if (this._view) {
      void this._view.webview.postMessage({
        type: "inspect-object",
        id: objectId,
      });
    }
  }

  private _getHtml(webview: vscode.Webview): string {
    const nonce = crypto.randomBytes(16).toString("hex");
    const cssUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._extensionUri, "sidebar", "catalog.css")
    );
    const jsUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._extensionUri, "out", "webview", "sidebar", "catalog.js")
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
  <div id="root"></div>
  <script nonce="${nonce}" src="${jsUri}"></script>
</body>
</html>`;
  }
}
