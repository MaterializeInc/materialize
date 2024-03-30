#!/usr/bin/env python3

import sys
import subprocess

# Disable coverage for protobuf-native as linking fails
if "protobuf_native" in sys.argv:
    try:
        sys.argv.remove("-Cpasses=sancov-module")
        sys.argv.remove("-Cllvm-args=-sanitizer-coverage-level=4")
        sys.argv.remove("-Cllvm-args=-sanitizer-coverage-inline-8bit-counters")
        sys.argv.remove("-Cllvm-args=-sanitizer-coverage-pc-table")
        sys.argv.remove("-Cllvm-args=-sanitizer-coverage-trace-compares")
        sys.argv.remove("-Cllvm-args=-sanitizer-coverage-stack-depth")
        sys.argv.remove("--cfg")
        sys.argv.remove("fuzzing")
        sys.argv.remove("-Clink-dead-code")
        sys.argv.remove("-Cdebug-assertions")
    except:
        pass
    print(sys.argv)
sys.exit(subprocess.call(sys.argv[1:], stdin=sys.stdin))
