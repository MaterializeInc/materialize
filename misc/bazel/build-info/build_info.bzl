# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Defines a Bazel rule that executes a Python script, that will generate a Rust
file with static values for all of the "workspace status" variables.
"""

def _impl(ctx):
    # Arguments that we pass to `gen_rust_module.py`.
    #
    # Bazel provides all of the 'volatile' variables in the 'version_file' and
    # 'stable' variables in the 'info_file', no idea why they chose this naming
    # scheme, but see <https://bazel.build/rules/lib/builtins/ctx#info_file>
    # for more info.
    args = ["--rust_file", ctx.outputs.rust_file.path] + \
           ["--volatile_file", ctx.version_file.path] + \
           ["--stable_file", ctx.info_file.path]

    ctx.actions.run(
        inputs = [ctx.version_file, ctx.info_file],
        outputs = [ctx.outputs.rust_file],
        arguments = args,
        progress_message = "Generating build info to {}".format(ctx.outputs.rust_file.path),
        executable = ctx.executable._gen_rust_module,
    )

gen_build_info = rule(
    implementation = _impl,
    attrs = {
        "_gen_rust_module": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//misc/bazel/build-info:gen_rust_module"),
        ),
        "rust_file": attr.output(mandatory = True),
    },
)
