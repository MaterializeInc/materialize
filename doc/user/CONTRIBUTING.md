This doc is a WIP. There is more useful information in the [README](./README.md).

# Set up

Rather than work on `github.com/materializeinc/materialize` itself, we recommend
working on your own fork of Materialize; this makes branches much tidier.
branches tidier.

1. Fork `github.com/materializeinc/materialize`. You can do this by going to
   that address and clicking **Fork**.
1. Move to the directory where you want to work, and clone your fork to your
   local machine:

    ```shell
    git clone git@github.com:<you>/materialize.git
    ```

1. Move into the `materialize` directory:

    ```shell
    cd materialize
    ```

1. Set `materializeinc/materialize` as your `upstream` remote:

    ```shell
    git remote add upstream git@github.com:MaterializeInc/materialize.git
    ```

This will give you your own workspace to use while still giving you the ability
to `git pull` the latest version of Materialize.

# Viewing the docs locally

1. [Install Hugo](https://gohugo.io/getting-started/installing/).

1. Launch the site by running:

    ```shell
    cd doc/user
    hugo serve -D
    ```

    If you're making changes to the site, you might want a more cache-busting
    version:

    ```shell
    hugo server --disableFastRender --ignoreCache
    ```

1. Open <http://localhost:1313>.

You can also read the documentation in Markdown in the `content` directory,
though some features like our SQL syntax diagrams will not be readily
accessible.
