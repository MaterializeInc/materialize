# Changelog

All notable changes to the `mz` CLI will be documented in this file.

## [0.1.3] - 2023-09-08

This new version contains a big revamp of the `mz` CLI. It includes updated code with an entirely different pattern for `profiles`.

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