# Internal apps

Console internal apps are self contained features that we are either
prototyping before promoting to user facing features, or features that are only
intended for internal use by engineers and support.

## Goals

The main goal is to make it an easy lift for any engineer to build something
they think is useful, so we can try it out without having to answer questions
like where it should live in our information architecture, or needing design
support. Also, Console has a lot of useful infrastructure for running queries
against Materialize, as well as impersonation, which allows us to try these
features out against customer environments.

## Adding a new app

To add a new internal app, start by adding a new route in the
`InternalRoutes.tsx` file, create new folder in `src/platform/internal/`, and
start building out your feature. Also, add a new `NavItem` to the
`InternalNavBar` for your feature. You should be able to use patterns seen in
other console features, such as our data fetching hooks and global Jotai state
to get information about the current environment.

## Promoting apps to user facing features

If we want to promote an internal feature to a user facing feature, it should
be fairly mechanical, we can move the code to a public route behind a feature
flag and add entry points in other parts of Console.
