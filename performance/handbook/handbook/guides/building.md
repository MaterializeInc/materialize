# Building and Developing on the Performance Handbook

## Developing The Handbook

This handbook is built using [MkDocs][], which has a builtin development server mode, enabling you
to preview your changes as you make them. It works by watching the markdown source files,
rebuilding the HTML files when they change and then sending an reload event to your browser so
that you can view them.

To start a preview server locally, in develop mode, run the following command:

    $(git rev-parse --show-toplevel)/performance/handbook/bin/develop

This command will block until you hit `CTRL-C`, at which point it will stop the development
server.

## Building the Handbook

TBD.
