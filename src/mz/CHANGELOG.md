# Changelog

All notable changes to the `mz` CLI will be documented in this file.

## [0.1.4] - 2023-09-04

This version implements fixes from internal feedback and pending features.

### Added
 - The command `mz profile init` includes a better interface to display the output in the browser.
 - The command `mz sql` enables timing by default.
 - The command `mz sql` displays the profile and cluster used for the connection.
 - The command `mz sql` now accepts `--cluster=<CLUSTER_NAME>` as option.
 - The option `--admin_endpoint` will auto-complete when `--cloud_endpoint` is available.

### Fixed
 - The command `mz profile init` now points to `console.materialize.com`.

### Changed
 - The profile names are restricted to consist of only ASCII letters, ASCII digits, underscores, and dashes.
 - The region creation and deletion timeouts have been increased and retry has been added.
 - The region option is now global.

### Removed
 - The option `--region` has been removed from subcommands.

## [0.1.3] - 2023-08-09

This version introduces a large code refactor of the client, as well as new functionality:

### Added
 - The `mz user` command for managing users in the organization.
 - The `mz config` command for managing the configuration parameters for the CLI.

### Changed
 - The `mz shell` command has been renamed to `mz sql`. It is no longer required to specify a region: the region is either retrieved from the configuration file (default), or can be passed as a parameter using the `region` flag: `mz sql --region='aws/us-east-1'`
 - The `mz login` command has been renamed to `mz profile`.

### Removed
 - The command `mz docs` has been removed.
 - The `vault` functionality has been temporarily removed, and will be restored in a future release.

Check [Keep a Changelog](http://keepachangelog.com/) for recommendations on how to structure this file.
