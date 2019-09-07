# Visual Studio Code support

This directory contains a [Visual Studio Code][vscode] extension that
provides some Materialize-specific language support. Features so far include:

  * Syntax highlighting for testdrive (.td) files
  * Syntax highlighting for Materialize's specific dialect of SQL (.mzsql)

Syntax highlighting for sqllogictest is also available via Nikhil's
[Sqllogictest][vscode-sqllogictest] extension in the public VSCode Extension
Marketplace.

## Installation

If you have VSCode installed:

```
ln -s /absolute/path/to/this/repo/misc/editor/vscode ~/.vscode/extensions
```

With some work, it's likely to possible to use these syntax highlighting grammar
files with other editors. Most modern editors (Atom, Sublime) all use the same
underlying TextMate grammar format, and the IntellJ family has a [TextMate
plugin][intellij-textmate] that may prove useful.

[vscode]: https://code.visualstudio.com
[intellij-textmate]: https://www.jetbrains.com/help/idea/tutorial-using-textmate-bundles.html
[vscode-sqllogictest]: https://marketplace.visualstudio.com/items?itemName=benesch.sqllogictest
