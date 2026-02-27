# Materialize web console

This is the web console interface for Materialize.

Doc index

- [Self-hosted Console Impersonation](misc/docker/README.md) - describes
  how to build and deploy a self-hosted version of the Console.
- [CI](doc/CI.md) - a high level overview of our CI/CD pipeline.
- [Architecture](doc/architecture.md) - a general overview of how Console
  talks to the rest of the Materialize.
- [Testing guide](doc/guide-testing.md) - a guide for testing in the Console.
- [Internal apps](doc/internal-apps.md) - how to build internal only features
  in console.
- [SSO/Frontegg Debugging](doc/testing-sso.md) - how to test OpenID Connect, SAML, and anything that can be done in Frontegg staging/production but not in local development.
- [Mz Backwards Compatibility](doc/mz-backwards-compatibility.md) - How to handle backwards compatibility during Materialize version changes
- [Organization Impersonation](doc/organization-impersonation.md) - describes
  our how console can impersonate customer organizations.
- [Design docs](doc/design/README.md) - details about our technical design
  doc process.

## Running the app locally

The specific versions of Node and Yarn are set for Volta in the package.json.
If you have another preferred version manager, please match the versions in the
`engines` section of the `package.json`.

```bash
corepack enable
yarn install
yarn start
open http://localhost:3000
```

This will run Console locally, pointing at our staging cloud resources. If a
`yarn` binary cannot be found after running `corepack enable`, you may need to
reshim your node version manager. [Corepack is not available with
Volta-distributed Node](https://github.com/volta-cli/volta/issues/987).

### Cloud setup

Clone the [Cloud repo](https://github.com/MaterializeInc/cloud) as a sibling to the materialize repo. We rely
on cloud for a few things:

- The mzadmin tool for getting cli access to AWS and configuring our k8s
  contexts.
- Running our E2E tests against a cloud stack running locally.
- The gen:api script uses cloud openapi specifications to generate our API
  clients. The OpenAPI specifications are defined in the cloud repo and referenced in `redocly.yaml`.

See their [Developer
doc](https://github.com/MaterializeInc/cloud/blob/main/doc/developer.md) for
the most up to date instructions on working with the cloud repo.

Below is a quick start guide for our use of cloud.

Install rust with [rustup](https://www.rust-lang.org/tools/install):

```shell
curl https://sh.rustup.rs -sSf | sh
```

Install Docker and make sure it's running: [https://docs.docker.com/engine/install/](https://docs.docker.com/engine/install/)

Install the [aws-cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

Install some k8s utils:

```shell
brew install kubectl@1.24 k9s kind
```

Run the commands below to configure your aws account and k8s contexts

```shell
# Installs dependencies and configures your python venv
bin/rev-env
# Setup your aws config.
bin/mzadmin aws setup
# Setup k8s contexts
bin/mzadmin k8s setup
```

If you encounter the following error:

```shell
WARNING:botocore.credentials:Refreshing temporary credentials failed during mandatory refresh period...
```

login to Pulumi via `bin/pulumi login`.

### Deleting environments

There is no way to disable a region in the UI, so if you need to test the
enable region flow, you will need to delete the environment via the api.

From the cloud repo, run:

```bash
# Generate a temporary aws auth token
bin/mzadmin aws login
# Materialize employees can see their org ID in the footer of the console
bin/mzadmin environment delete --cluster staging-us-east-1 --organization $ID
```

## Building for production

Since we use Vite, our local development build is significantly different than the
production build, so it's sometimes useful to run a production build locally. First,
build the app with `yarn build:local`, and then run `yarn preview` to run a static
server, which just serves the `/dist` directory.

## Generating latest database types

To generate TypeScript types from the Materialize system catalog tables:

1. Run `yarn gen:types` to fetch the latest emulator (`materialized`) and generate types using `kysely-codegen`
2. Optionally specify a Materialize version (e.g. `yarn gen:types v0.148.0`)
3. Make sure port 6875 is available before running the command

## Icons

We use `vite-plugin-svgr` to automatically turn simple SVG imports into Chakra Icon
components, which allows us to use dynamic colors and Chakra styling properties. To
ensure consistent naming and easy bundle splitting, all icons are exported from an icon
index file. To add a new icon, just drop the svg into the `icon` directory, then add an
export to `icon/index.tsx`.

## Theming

Materialize Console has light and dark mode support. Some of the styling
for this is implicit (based on Chakra's defaults). Other parts were customized
to match Materialize's styles and color scheme. When styling, make sure that
components look good in both modes. On Mac OS, you can toggle your color mode
with System Preferences \> General.

### Customizing

Chakra's theme docs live
[here](https://chakra-ui.com/docs/theming/customize-theme). There are two
methods by which we can customize our application's look and feel using Chakra:

#### App-wide styles and defaults in `src/theme/`

If your styling should apply to all instances of a built-in Chakra component,
across the board, it's best to modify the Chakra theme. Our custom color
palette is in `theme/colors.ts`, and imported and supplemented in
`theme/index.ts`. Customizations of reusable Chakra components live in
`theme/components.ts`, including setting default variant props. You can crib
off of the existing component styles there, and/or look up the
[sources for Chakra UI's default
theme](https://github.com/chakra-ui/chakra-ui/tree/main/packages/theme/src/components).
The Chakra docs for custom component theming, especially with dark mode in the
mix, are unfortunately not very thorough.

#### Component-specific styling with `useColorMode` and `useColorModeValue`

For one-off styles, especially to components that we built ourselves (that
aren't part of the Chakra system), you can use [Chakra's `useColorMode` and
`useColorModeValue` hooks](https://chakra-ui.com/docs/features/color-mode)
to swap between colors or other styles. `useColorModeValue` is preferred for
singular values (e.g. a single color)--if you need a whole long complicated
string or are switching many different variables, `useColorMode` might make
more sense.

It is preferred to not create a custom component wrapping a Chakra component to
add styles; this should happen in the aforementioned theme file instead.

### Theme testing

On Mac, you can set your color mode in `System Preferences > Settings`. When
you change your color mode, the site will dynamically switch styles. This makes
it pretty easy to check how your new feature works in either style.

## Images

Static images live in `img` and use imports to get added to their
parent component. For SVGs where one wants to customize attributes such as fill
colors, however, importing will not work; in that case the SVG should get added
to `src/svg` as a React component wherein that customization (for
light/dark mode, or for reuse) can happen.

## Code style

We use Prettier to format all files, enforced by eslint-plugin-prettier.
Configure your editor to format files on save and you generally won't have to
think much about formatting.

### Pre-commit hook (optional)

To automatically run ESLint on staged files before each commit, you can enable
the pre-commit hook:

```bash
# From the materialize repo root:
ln -s ../../console/misc/githooks/pre-commit .git/hooks/pre-commit
```

### Component files

At Materialize is acceptable to put multiple components in the same file, when
one is clearly consumed by another (and if, in your judgment, the file is not
too ungainly or complex).

There are exceptions, such as the various pieces of our `<Card>`s, where a
component must be in multiple "lego blocks" that fit together. When this happens
the file name should reflect the fact (e.g. `cardComponents.tsx` rather than
`Card.tsx`).

Don't name components index.tsx (other than the root of the application, of
course).

- This saves us from extraneous folders and
- This makes it easier to tell at a glance which component is which (if the
  title of every tab in your editor is index.tsx, that is really confusing!).

Name the props for your each component `$ComponentProps`, export it and put it
above the component that uses it.
