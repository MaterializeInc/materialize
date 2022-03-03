This doc walks you through the process of setting up your environment to
contribute to Materialize's documentation.

This doc is a WIP. There is more useful information in the [README](./README.md).

# Set up

You should (or must) perform your work on your own fork of Materialize.

1. Fork <https://github.com/materializeinc/materialize>. You can do this by going to
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

# View the docs locally

1. [Install Hugo](https://gohugo.io/getting-started/installing/).

1. Launch the site by running:

    ```shell
    cd doc/user
    hugo server -D
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
