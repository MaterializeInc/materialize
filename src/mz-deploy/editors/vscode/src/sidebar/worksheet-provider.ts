/**
 * WebviewViewProvider for the mz-deploy SQL worksheet panel.
 *
 * Manages the webview lifecycle, CSP, and message routing between the
 * worksheet webview and the extension host. The extension host relays
 * execute/cancel requests to the LSP server and pushes results back
 * via postMessage.
 */

import * as vscode from "vscode";
import * as crypto from "crypto";
import {
  WorksheetOutboundMessage,
  WorksheetInboundMessage,
} from "../types";

export class WorksheetProvider implements vscode.WebviewViewProvider {
  private _extensionUri: vscode.Uri;
  private _view: vscode.WebviewView | null;
  private _onMessage: ((msg: WorksheetOutboundMessage) => void) | null;

  constructor(extensionUri: vscode.Uri) {
    this._extensionUri = extensionUri;
    this._view = null;
    this._onMessage = null;
  }

  resolveWebviewView(
    webviewView: vscode.WebviewView,
    _context: vscode.WebviewViewResolveContext,
    _token: vscode.CancellationToken
  ): void {
    this._view = webviewView;

    webviewView.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(this._extensionUri, "panels"),
        vscode.Uri.joinPath(this._extensionUri, "out", "webview", "panels"),
      ],
    };

    webviewView.webview.html = this._getHtml(webviewView.webview);

    webviewView.webview.onDidReceiveMessage(
      (msg: WorksheetOutboundMessage) => {
        if (this._onMessage) {
          this._onMessage(msg);
        }
      }
    );
  }

  onMessage(callback: (msg: WorksheetOutboundMessage) => void): void {
    this._onMessage = callback;
  }

  postMessage(msg: WorksheetInboundMessage): void {
    if (this._view) {
      void this._view.webview.postMessage(msg);
    }
  }

  private _getHtml(webview: vscode.Webview): string {
    const nonce = crypto.randomBytes(16).toString("hex");
    const cssUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this._extensionUri, "panels", "worksheet.css")
    );
    const jsUri = webview.asWebviewUri(
      vscode.Uri.joinPath(
        this._extensionUri,
        "out",
        "webview",
        "panels",
        "worksheet.js"
      )
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
