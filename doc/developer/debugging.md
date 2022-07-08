# Debugging guide

*Last updated November 5th, 2019*

This page details various tools and techniques we use to debug the Materialize
software stack / Rust programs more generally.

## Overview

There's a couple of different options for debugging problems with Rust programs

  1. Printing more data / printing backtraces

  2. Using a debugger

## Printing more data

## Debuggers

Rust provides wrappers over `gdb` and `lldb` called `rust-gdb` and `rust-lldb`.
These wrappers mainly add better pretty printing for Rust objects and limited
support for parsing Rust expressions in the debugger's REPL. On macOS,
`rust-lldb` should be ready to go pretty much out of the box but `rust-gdb`
will require some setup. Conversely, on Linux, `rust-gdb` should be available
out of the box and `rust-lldb` is currently unavailable.

At this time, we don't have a clear recommendation between gdb and lldb. Sometimes,
gdb provides more useful introspection than lldb. Other times, gdb hangs and is
unable to debug but lldb works. The best recommendation we have at the moment
is to start with the tool thats easiest on each platform (lldb for macOS, gdb
for Linux) and only bother setting up gdb on macOS if lldb fails. Either way,
it's good to have the option to use either tool, and any technique used on
one debugger will translate to the other.

### rust-gdb setup on macOS

First, install `gdb` with
```shell
$ brew install gdb
```

Verify that you have `gdb` version 8.3 or higher by checking
```shell
$ gdb -v
```
If you don't then you may have to update Homebrew [TODO source] or build `gdb`
from source [TODO source]

Once you have a recent `gdb` version create a file called `.gdbinit` in your
home
directory and add the following line to it
```
set startup-with-shell disable
```

We're almost done setting up `gdb`. Now we just need to code sign the binary.
First, we need to create a new certificate.

  1. Open up Keychain Access
  2. Select Certificate Assistant -> Create a Certificate
  3. Give it a name like `gdb-cert` (You can call it whatever you like just
     substitute that name in the rest of the instructions)
  4. Set "type" to "Self-signed root"
  5. Set "Certificate Type" to "Code Signing"
  6. Select "Let me override defaults"
  7. Scroll through and select default options for everything (e.g. key size,
     encryption algorithm)
  8. For the "Location" option choose "System"
  9. Restart the machine (it may work without the restart but the Internet
     recommends it)

After we've made the certificate we need to make an entitlements file (these
are formatted as XML).
```shell
cat gdb.xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.cs.allow-jit</key>
    <true/>
    <key>com.apple.security.cs.allow-unsigned-executable-memory</key>
    <true/>
    <key>com.apple.security.cs.allow-dyld-environment-variables</key>
    <true/>
    <key>com.apple.security.cs.disable-library-validation</key>
    <true/>
    <key>com.apple.security.cs.disable-executable-page-protection</key>
    <true/>
    <key>com.apple.security.cs.debugger</key>
    <true/>
    <key>com.apple.security.get-task-allow</key>
    <true/>
</dict>
</plist>
```

Once you have an entitlements file (example name is `gdb.xml`) all you have to
do is
```shell
codesign --entitlements gdb.xml -fs gdb-cert /usr/local/bin/gdb
```

We are finally ready to start debugging!

### Using rust-{gdb,lldb}

First, we need to see what binaries need to be debugged. Adding `-v` to Cargo
commands should give the necessary info. After running for example
`cargo test -v`

```shell
    Running `/Users/Test/github/sqlparser/target/debug/deps/sqlparser-726083e10aabeebf`
```
The path after "Running" is the binary - to validate that just run
```shell
$ /path/to/binary
```

We can now use `rust-gdb` (or `rust-lldb`) by simply doing
```shell
$ rust-gdb /path/to/binary
```

There's a few more wrinkles / tips and tricks for running unit tests.

The Rust test runner (what we get when we call `cargo test`) defaults to
spawning multiple unit tests in parallel. We can use the `RUST_TEST_THREADS`
environment variable to limit it to one thread which is easier to debug.

```shell
$ RUST_TEST_THREADS=1 rust-gdb /path/to/binary
```

Finally, adding breakpoints can make the debugger execution really slow it
(chalk it up to Rust unoptimized builds + debugger overhead maybe?) We can pass
the name of the test we want to run as a command line arg
```shell
$ RUST_TEST_THREADS=1 rust-gdb --args /path/to/binary <test_name>
```

The corresponding command for `lldb` is:
```shell
$ RUST_TEST_THREADS=1 rust-lldb -- /path/to/binary <test_name>
```

In general, GDB and LLDB commands have the annoying property of being very
similar, but not identical. This [page](https://lldb.llvm.org/use/map.html) maps
commands from one debugger to another. At this point, we are free to do all
sorts of things with our debugger - set breakpoints, inspect memory, and step
through code execution to name just a few examples.

### Debugging forked child processes with gdb
GDB does not automatically attach to forked child processes, which can make
debugging Materialize more difficult. In order to attach to child processes
you first need to tell GDB to attach to both processes on fork:
```gdb
(gdb) set detach-on-fork off
```
Then you need to tell GDB to follow the child process on fork:
```gdb
(gdb) set follow-fork-mode child
```
These commands can be saved in a [`.gdbinit`](https://sourceware.org/gdb/onlinedocs/gdb.html#Initialization-Files)
file for convenience.

When a child process exits, you will need to manually switch back to the parent
process to resume execution. You should see an output that looks like this:
```gdb
[Inferior 3 (process 615551) exited with code 01]
```
Inferior number and process number will likely be different. GDB calls any
program it executes an inferior.

To see all the current inferiors, run:
```gdb
(gdb) info inferiors
  Num  Description       Executable
  1    process 615534    /home/user/materialize/target/debug/materialized
  2    process 615550    /usr/bin/python3.8
* 3    <null>            /usr/bin/dpkg-query
```
Your output will likely vary, but the current dead process will be prefixed with `*` and have a description of `<null>`.

To continue debugging you will need to switch to the parent process and continue execution:
```gdb
(gdb) inferior 2
[Switching to inferior 2 [process 615550] (/usr/bin/python3.8)]
[Switching to thread 2.1 (Thread 0x7ffff7bf2740 (LWP 615550))]
#0  arch_fork (ctid=0x7ffff7bf2a10) at ../sysdeps/unix/sysv/linux/arch-fork.h:49
49      ../sysdeps/unix/sysv/linux/arch-fork.h: No such file or directory.
(gdb) continue
Continuing.
```
Again, your inferior number and output will likely be different.

### Further information

The rustc
[guide](https://rustc-dev-guide.rust-lang.org/debugging-support-in-rustc.html)
has a page detailing the state of debugger support in Rust. There's plenty of
guides for how to use GDB/LLDB online but most of them focus on C/C++ programs.
[Beej's Quick Guide to GDB](https://beej.us/guide/bggdb/) is a good starting
point.
