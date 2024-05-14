# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

load("@bazel_skylib//lib:paths.bzl", "paths")

"""
select_path() build rule implementation.

Selects a single path from the outputs of a target, by given relative path.
"""

def find_suffix(path, suffix):
    """Walks up a path trying to match suffix, returning None if it cannot."""
    if not path or len(path) == 0:
        return None

    # Note: Starlark does not support recursion or `while` loops, so we need to
    # do this janky thing.
    fixed_path = path.replace("\\", "/")
    component_count = len(fixed_path.split("/"))
    
    for x in range(component_count):
        if fixed_path.endswith(suffix):
            return fixed_path
        else:
            fixed_path = paths.dirname(fixed_path)

    return None

def _select_path_impl(ctx):
    if ctx.attr.suffix and len(ctx.attr.suffix) == 0:
        fail("Subpath can not be empty.")

    final_path = None
    canonical = ctx.attr.suffix.replace("\\", "/")

    for f in ctx.attr.srcs.files.to_list():
        candidate = find_suffix(f.path, canonical)
        if candidate:
            final_path = paths.join(f.root.path, candidate)
            break

    if not final_path:
        files_str = ",\n".join([
            str(f.path)
            for f in ctx.attr.srcs.files.to_list()
        ])
        fail("Can not find specified file in [%s]" % files_str)

    print("%s", final_path)

    out = ctx.actions.declare_symlink(canonical)
    ctx.actions.symlink(output=out, target_path = final_path)

    return [DefaultInfo(files = depset([out]))]

select_path = rule(
    implementation = _select_path_impl,
    doc = "Selects a single path from the outputs of a target by matching the path suffix",
    attrs = {
        "srcs": attr.label(
            allow_files = True,
            mandatory = True,
            doc = "The target producing the path among other outputs.",
        ),
        "suffix": attr.string(
            mandatory = True,
            doc = "Suffix of the path you want to match.",
        ),
    },
)
