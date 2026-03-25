---
source: src/s3-datagen/src/main.rs
revision: e757b4d11b
---

# mz-s3-datagen

A CLI tool that generates bulk test data in an S3 bucket to measure download throughput.
It uploads one initial object composed of repeated fixed-length lines, then concurrently copies it a configurable number of times using the S3 server-side copy API.
Concurrency, object size, line size, bucket, key prefix, and region are all configurable via command-line flags.
