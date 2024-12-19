#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Regenerate lint annotations on all root Cargo workspace files and check that
all crates inherit their lints from the workspace."""

import argparse
import json
import subprocess
from pathlib import Path

import toml

ALLOW_RUST_LINTS = [
    # Allows us to allow/deny new lints and support older versions of rust/clippy.
    "unknown_lints",
    "non_local_definitions",
]

ALLOW_RUST_DOC_LINTS = [
    # Allows us to use footnotes in rustdoc.
    "unportable_markdown"
]


ALLOW_CLIPPY_LINTS = [
    # The style and complexity lints frustrated too many engineers and caused
    # more bikeshedding than they saved. These lint categories are largely a
    # matter of opinion. A few of the worthwhile lints in these categories are
    # reenabled in `DENY_LINTS` below.
    ("style", -1),
    ("complexity", -1),
    # clippy::large_enum_variant complains when enum variants have divergent
    # sizes, as the size of an enum is determined by the size of its largest
    # element. Obeying this lint is nearly always a premature optimization,
    # and the suggested solution—boxing the large variant—might actually result
    # in slower code due to the allocation.
    ("large_enum_variant", 0),
    # clippy::mutable_key_type disallows using internally mutable types as keys
    # in `HashMap`, because their order could change. This is a good lint in
    # principle, but its current implementation is too strict -- it disallows
    # anything containing an `Arc` or `Rc`, for example.
    ("mutable_key_type", 0),
    # Unstable sort is not strictly better than sort, notably on partially
    # sorted inputs.
    ("stable_sort_primitive", 0),
    # This lint has false positives where the pattern cannot be avoided without
    # cloning the key used in the map.
    ("map_entry", 0),
    # It is unclear if the performance gain from using `Box::default` instead of
    # `Box::new` is meaningful; and the lint can result in inconsistencies
    # when some types implement `Default` and others do not.
    # TODO(guswynn): benchmark the performance gain.
    ("box_default", 0),
    # This suggestion misses the point of `.drain(..).collect()` entirely:
    # to keep the capacity of the original collection the same.
    ("drain_collect", 0),
]

WARN_CLIPPY_LINTS = [
    # Comparison of a bool with `true` or `false` is indeed clearer as `x` or
    # `!x`.
    "bool_comparison",
    # Rewrite `x.clone()` to `Arc::clone(&x)`.
    # This clarifies a significant amount of code by making it visually clear
    # when a clone is cheap.
    "clone_on_ref_ptr",
    # These can catch real bugs, because something that is expected (a cast, a
    # conversion, a statement) is not happening.
    "no_effect",
    "unnecessary_unwrap",
    # Prevent code using the `dbg!` macro from being merged to the main branch.
    #
    # To mark a debugging print as intentional (e.g., in a test), use
    # `println!("{:?}", obj)` instead.
    "dbg_macro",
    # Prevent code containing the `todo!` macro from being merged to the main
    # branch.
    #
    # To mark something as intentionally unimplemented, use the `unimplemented!`
    # macro instead.
    "todo",
    # Wildcard dependencies are, by definition, incorrect. It is impossible
    # to be compatible with all future breaking changes in a crate.
    "wildcard_dependencies",
    # Zero-prefixed literals may be incorrectly interpreted as octal literals.
    "zero_prefixed_literal",
    # Purely redundant tokens.
    "borrowed_box",
    "deref_addrof",
    "double_must_use",
    "double_parens",
    "extra_unused_lifetimes",
    "needless_borrow",
    "needless_question_mark",
    "needless_return",
    "redundant_pattern",
    "redundant_slicing",
    "redundant_static_lifetimes",
    "single_component_path_imports",
    "unnecessary_cast",
    "useless_asref",
    "useless_conversion",
    # Very likely to be confusing.
    "builtin_type_shadow",
    "duplicate_underscore_argument",
    # Purely redundant tokens; very likely to be confusing.
    "double_neg",
    # Purely redundant tokens; code is misleading.
    "unnecessary_mut_passed",
    # Purely redundant tokens; probably a mistake.
    "wildcard_in_or_patterns",
    # Transmuting between T and T* seems 99% likely to be buggy code.
    "crosspointer_transmute",
    # Confusing and likely buggy.
    "excessive_precision",
    "panicking_overflow_checks",
    # The `as` operator silently truncates data in many situations. It is very
    # difficult to assess whether a given usage of `as` is dangerous or not. So
    # ban it outright, to force usage of safer patterns, like `From` and
    # `TryFrom`.
    #
    # When absolutely essential (e.g., casting from a float to an integer), you
    # can attach `#[allow(clippy::as_conversion)]` to a single statement.
    "as_conversions",
    # Confusing.
    "match_overlapping_arm",
    # Confusing; possibly a mistake.
    "zero_divided_by_zero",
    # Probably a mistake.
    "must_use_unit",
    "suspicious_assignment_formatting",
    "suspicious_else_formatting",
    "suspicious_unary_op_formatting",
    # Legitimate performance impact.
    "mut_mutex_lock",
    "print_literal",
    "same_item_push",
    "useless_format",
    "write_literal",
    # Extra closures slow down compiles due to unnecessary code generation
    # that LLVM needs to optimize.
    "redundant_closure",
    "redundant_closure_call",
    "unnecessary_lazy_evaluations",
    # Provably either redundant or wrong.
    "partialeq_ne_impl",
    # This one is a debatable style nit, but it's so ingrained at this point
    # that we might as well keep it.
    "redundant_field_names",
    # Needless unsafe.
    "transmutes_expressible_as_ptr_casts",
    # Needless async.
    "unused_async",
    # Disallow the methods, macros, and types listed in clippy.toml;
    # see that file for rationale.
    "disallowed_methods",
    "disallowed_macros",
    "disallowed_types",
    # Implementing `From` gives you `Into` for free, but the reverse is not
    # true.
    "from_over_into",
    # We consistently don't use `mod.rs` files.
    "mod_module_files",
    "needless_pass_by_ref_mut",
]

MESSAGE_LINT_MISSING = (
    '{} does not contain a "lints" section. Please add: \n[lints]\nworkspace = true'
)
MESSAGE_LINT_INHERIT = "The lint section in {} does not inherit from the workspace, "

EXCLUDE_CRATES = ["workspace-hack"]

CHECK_CFGS = "bazel, stamped, coverage, nightly_doc_features, release, tokio_unstable"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--extra-dirs",
        action="append",
        default=[],
    )
    args = parser.parse_args()

    lint_config = [
        "\n" "# BEGIN LINT CONFIG\n",
        "# DO NOT EDIT. Automatically generated by bin/gen-lints.\n",
        "[workspace.lints.rust]\n",
        *(f'{lint} = "allow"\n' for lint in ALLOW_RUST_LINTS),
        f"unexpected_cfgs = {{ level = \"warn\", check-cfg = ['cfg({CHECK_CFGS})'] }}\n",
        "\n",
        "[workspace.lints.rustdoc]\n",
        *(f'{lint} = "allow"\n' for lint in ALLOW_RUST_DOC_LINTS),
        "\n",
        "[workspace.lints.clippy]\n",
        *(
            f'{lint} = {{ level = "allow", priority = {priority} }}\n'
            for (lint, priority) in ALLOW_CLIPPY_LINTS
        ),
        *(f'{lint} = "warn"\n' for lint in WARN_CLIPPY_LINTS),
        "# END LINT CONFIG\n",
    ]

    for workspace_root in [".", *args.extra_dirs]:
        workspace_cargo_toml = Path(f"{workspace_root}/Cargo.toml")

        # Make sure the workspace Cargo.toml files have the lint config.
        contents = workspace_cargo_toml.read_text().splitlines(keepends=True)
        try:
            # Overwrite existing lint configuration block.
            start = contents.index(lint_config[1]) - 2
            end = contents.index(lint_config[-1])
            contents[start : end + 1] = lint_config
        except ValueError:
            # No existing lint configuration block. Add a new one to the end
            # of the file.
            contents.extend(lint_config)
        workspace_cargo_toml.write_text("".join(contents))

        # Make sure all of the crates in the workspace inherit their lints.
        metadata = json.loads(
            subprocess.check_output(
                [
                    "cargo",
                    "metadata",
                    "--no-deps",
                    "--format-version=1",
                    f"--manifest-path={workspace_root}/Cargo.toml",
                ]
            )
        )
        cargo_toml_paths = (
            Path(package["manifest_path"])
            for package in metadata["packages"]
            if package["name"] not in EXCLUDE_CRATES
        )

        for cargo_file in cargo_toml_paths:
            cargo_toml = {}
            with cargo_file.open() as rcf:
                cargo_toml = toml.load(rcf)

            # Assert the Cargo.toml contains a "lints" section.
            assert "lints" in cargo_toml, MESSAGE_LINT_MISSING.format(cargo_file)
            # Assert the lints section contains a "workspace" key.
            assert cargo_toml["lints"].get("workspace"), MESSAGE_LINT_INHERIT.format(
                cargo_file
            )


if __name__ == "__main__":
    main()
