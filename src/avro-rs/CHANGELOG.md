# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.5] - 2019-03-09
### Fixed
- Allow Array(Int) to be converted to Bytes
- Fix enum type deserialization bug

## [0.6.4] - 2018-12-24
### Fixed
- Variable-length encoding for big i64 numbers

## [0.6.3]- 2018-12-19
### Added
- Schema fingerprint (md5, sha256) generation

## [0.6.2]- 2018-12-04
### Fixed
- Snappy codec

## [0.6.1]- 2018-10-07
### Fixed
- Encoding of i32/i64

## [0.6.0]- 2018-08-11
### Added
- impl Send+Sync for Schema (non-backwards compatible)

## [0.5.0] - 2018-08-06
### Added
- A maximum allocation size when decoding
- Support for Parsing Canonical Form
- `to_value` to serialize anything that implements Serialize into a Value
- Full support for union types (non-backwards compatible)
### Fixed
- Encoding of empty containers (array/map)

## [0.4.1] - 2018-06-17
### Changed
- Implememented clippy suggestions

## [0.4.0] - 2018-06-17
### Changed
- Many performance improvements to both encoding and decoding
### Added
- New public method extend_from_slice for Writer
- serde_json benchmark for comparison
- bench_from_file function and a file from the goavro repository for comparison

## [0.3.2] - 2018-06-07
### Added
- Some missing serialization fields for Schema::Record

## [0.3.1] - 2018-06-02
### Fixed
- Encode/decode Union values with a leading zig-zag long

## [0.3.0] - 2018-05-29
### Changed
- Move from string as errors to custom fail types

### Fixed
- Avoid reading the first item over and over in Reader

## [0.2.0] - 2018-05-22
### Added
- `from_avro_datum` to decode Avro-encoded bytes into a `Value`
- Documentation for `from_value`

## [0.1.1] - 2018-05-16
- Initial release
