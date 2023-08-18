# Change Log

All notable changes to the `mz` CLI will be documented in this file.

## [0.1.3] - 2023-09-08

This new version contains a big revamp of the `mz` CLI. It includes updated code with an entirely different pattern for `profiles`.

### Added
 - Added the `mz user` command for managing users in the organization.
 - Added the `mz config` command for managing the configuration parameters for the CLI.

### Changed
 - The `mz shell` command is now `mz sql`, featuring newer code and doesn't requires to specify a region anymore. The region will be retrieved from the configuration file or can be pass by parameter: `mz sql --region='aws/us-east-1'`
 - The `mz secret` command now includes updated code.
 - The `mz login` is now `mz profile`, and it is for managing the authentication profiles for the CLI.

### Removed
 - The command `mz docs` is no longer available.
 - The `vault` functionality is temporarily unavailable, and work is in progress to bring it back.

Check [Keep a Changelog](http://keepachangelog.com/) for recommendations on how to structure this file.