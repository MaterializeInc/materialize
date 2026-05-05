# Changelog

All notable changes to the `mz` CLI will be documented in this file.

## [0.3.1] - 2025-11-04

Added `--environmentd-cpu-allocation` and `--environmentd-memory-allocation` to `mz region enable`. These are for internal use only.

## [0.3.0] - 2023-10-26

This version includes a more secure app-password storage for macOS, and extends current features. Migration from old profiles to new ones will happen automatically.

### Added
 - Support to store app-passwords in the keychain for macOS.
 - A new vault field in the configuration file has been added to indicate if keychain usage.

### Changed
 - The command `mz app-password create` does not require a name anymore.
 - The command `mz profile init` requires a `--force` option if a profile already exists.

## [0.2.2] - 2023-09-25

This version implements a change to use a new API version '1' of the Region API, and usability improvements.

### Added
 - Added support to upper case region name.

### Changed
 - The command `mz region enable` was updated to use the newly introduced `regionState` field returned from the Region API to display more informative loading states.
 - The command `mz region list` was updated to use the new API response semantics of the Region API in version 1.
 - The profile option is now a global option.

### Removed
 - Deprecated code has been removed.

## [0.2.1] - 2023-09-07

This version only implements changes in the release process.

### Added
 - A new file in the binaries endpoint storing the latest `mz` version.

## [0.2.0] - 2023-09-04

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
