---
title: "VS Code Extension"
description: "Guide to using the Materialize VS Code extension for development"
aliases:
  - /third-party/vscode-extension/
  - /integrations/vscode-extension/
menu:
  main:
    parent: "integrations"
    name: "VS Code Extension"
    weight: 20
---

The Materialize VS Code extension enhances your development workflow by integrating directly with Visual Studio Code, allowing you to explore, query, and interact with your Materialize instances without leaving your favorite IDE.

## Installation

Install the Materialize VS Code extension directly from the Visual Studio Code Marketplace.

Search for **Materialize** in VS Code's Extensions view (`Ctrl+Shift+X` or `Cmd+Shift+X` on macOS) and click **Install**.

Alternatively, you can find more information and the installation link in the [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=materialize.vscode-materialize).

![Materialize VS Code Extension Overview](https://github.com/MaterializeInc/vscode-extension/blob/main/media/Extension.png?raw=true)

## Authentication

To use the Materialize VS Code extension, you need to authenticate with your Materialize account.

After installing the extension, follow these steps to authenticate:

1. In VS Code, click the Materialize icon in the Activity Bar to open the extension's view.

1. Choose a descriptive name for your profile, such as **Production** or **Staging**.

1. Click the **Continue** button and open the Materialize authentication page in your browser.

1. Log in to your Materialize account and select the region that you want to connect to.

1. When prompted to create an app password for the extension, click **Yes**.

1. Return to VS Code. Your profile is now authenticated and ready to use.

## Features

The extension provides the following features to enhance your development experience:

### Schema Explorer

Navigate through your schemas directly within VS Code.

The Schema Explorer provides a tree view of all your sources, views, and the system catalog, displaying each object's name, columns, and types. You can copy object names to the clipboard with a single click, simplifying the process of writing queries.

![Materialize VS Code Extension Schema Explorer](https://res.cloudinary.com/mzimgcdn/image/upload/v1697472608/SchemaExplorer.gif)

### Profile Configuration

The extension allows you to authenticate and manage multiple connection profiles using your browser.

You can switch between profiles and adjust connection options, such as cluster, database, or schema, enhancing the flexibility of your development environment.

![Materialize VS Code Extension Profile Switcher](https://res.cloudinary.com/mzimgcdn/image/upload/v1697472608/ProfileSwitch.gif)

### Query Execution

Execute SQL queries and run `.sql` files with ease. Select your SQL code and press the Materialize play button or use the shortcut `⌘ Cmd + ⤶ Enter` (on macOS) to run your query.

Query results are displayed in a dedicated bottom panel, allowing for quick insights and analysis.

![Materialize VS Code Extension Query Execution](https://res.cloudinary.com/mzimgcdn/image/upload/v1697472608/Queries.gif)

### Validation

Materialize's specific commands and syntax extensions to PostgreSQL can be tricky.

The VS Code extension includes SQL validation that uses Materialize's parser to catch syntax errors and provide real-time diagnostics, ensuring your queries are correct before execution.

## Additional Resources

Before using the extension, ensure you have a Materialize account. Register [here](https://materialize.com/signup) to access all features. Once set up, you can begin exploring your Materialize data directly within VS Code.

If you are interested in contributing or exploring the extension's code, you can check out the [GitHub repository](https://github.com/MaterializeInc/vscode-extension).
