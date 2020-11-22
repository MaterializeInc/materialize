# Building and Developing on the Performance Handbook

This document details how to work on the performance handbook directly, as well as how the build
process works and the tools that it depends on.

## Developing The Handbook

To develop this documentation locally, do the following:

!!! todo

    The instructions for running a local development server have not been ported over yet

### Fast Linting

While you can use our GitHub Actions or `earth +lint` to verify that the handbook lints cleanly,
neither of these steps are particularly fast. Personally, I like to use a tool called [reflex][]
to watch for local changes to markdown files and reruns `mdl`.

```sh
cd "$(git rev-parse --show-toplevel)/handbook"
reflex --all -r '\.md$' -- sh -c "clear; echo `date`; mdl -s mdl_style.rb handbook"
```

Now, anytime you save a file, not only will your browser reload, but your terminal window will
also show any lint errors. The `clear` statement insures that only current errors show up in your
terminal and the `date` command lets you know when the command was last run.

!!! caution

    Be aware that the Handbook lint process used for validating pull requests may change, either
    by moving to new versions of `mdl` or perhaps choosing another tool. Additionally, updates to
    your local system may result in your using newer versions of the tool and those versions may
    behave differently. While we will strive to keep this documentation up to date, the output of
    the above command may drift from the output of `earth +lint`.

[reflex]: https://github.com/cespare/reflex

## Tools Used

The handbook is built using a combination of 3 tools: `mkdocs`, `docker` and `earthly`. Let's go
over why each tool was chosen and what it does.

### mkdocs
[mkdocs](https://www.mkdocs.org/) is a static site generator that converts markdown to HTML. There
is a nice [mkdocs-material](https://squidfunk.github.io/mkdocs-material/) theme that looks pretty
nice. It can also be converted to pdf output using
[mkdocs-pdf-export-plugin](https://github.com/zhaoterryy/mkdocs-pdf-export-plugin) if so desired.

### Docker
In addition to requiring specific Python versions, this build also relies on Ruby for
[mdl](https://github.com/markdownlint/markdownlint) and Nginx for hosting the static site. By
using Docker, we can avoid requiring that each user install the specific system dependencies
required for each of these builds.

### Earthly
It would be a shame if we needed to install Python, Ruby and Nginx all into the same container
and/or manage an unwiedly chain of docker build commands to copy artifacts to/from images.
[Earthly](https://github.com/earthly/earthly) is a meta-build tool that builds artifacts using
docker images. By using Earthly, we can easily save the static site built during the `mkdocs
build` step and copy it into Nginx container during it's build. The `mkdocs build` step is
conditional upon having a clean lint. If anything required to run the linter or build steps
change, then running the Ngxinx container build will automatically rerun those steps.

It's like Make or Cmake but written using containers.

## One-Time Setup

- Install Docker on your system

!!! todo

    Instructions for installing Earthly are not yet documented

## Building Artifacts Directly

This is the advanced section. You likely just want to run the steps under [Developing The
Handbook](#developing-the-handbook) to start the local server. The linter and build steps will
soon be run as GitHub Actions.

Using Earth directly, there are 4 end-user targets defined:

- `build`: The `mkdocs` step that builds the static site
- `develop`: Build the Docker image used for live-reloading during development
- `lint`: Run `mdl` (`markdownlint`) over the source code looking for style errors
- `server`: Build the Docker image containing Nginx, serving the static site

### Running the Linter Directly

To lint the documentation, do the following:

```sh
cd "$(git rev-parse --show-toplevel)/performance/handbook"
earth +lint
```

### Running the Static Site

To see how the final product looks, do the following:

```sh
cd "$(git rev-parse --show-toplevel)/performance/handbook"
earth +server
docker run -p 80:8000 materializeinc/performance/handbook:latest
```

We now have a container that we can `docker push` to a private registry for sharing / deployment!
