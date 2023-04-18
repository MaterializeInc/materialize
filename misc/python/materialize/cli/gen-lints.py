#!/usr/bin/env python3

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Regenerate lint annotations on all root Rust files.

Rust and Clippy do not permit configuring the allowed/denied lints per
workspace. See [0] for a longstanding feature request. Instead they must be
configured per target--i.e., individually configured via attributes in every
binary, example, test, and library crate.

This script automatically installs the list of lints we want to allow/deny into
each such target. This adds a lot of noise to each crate, but we think the noise
is worth it, as all Rust tools know how to interpret these lint attributes.

We previously used a shell script, named `bin/check`, to instead invoke `cargo
clippy --all-targets` with our desired lint configuration. This avoided the
noise, but did not integrate well with editors, which would be unaware of our
lint configuration. Clippy-enabled editors would flag lints that were disabled
in CI, and elide lints that were required in CI.

`include!` does not help here, as `include!` does not support including files
with inner attributes [1].

[0]: https://github.com/rust-lang/cargo/issues/5034
[1]: https://github.com/rust-lang/rfcs/issues/752
"""

import json
import subprocess
from pathlib import Path

ALLOW_LINTS = [
    # The style and complexity lints frustrated too many engineers and caused
    # more bikeshedding than they saved. These lint categories are largely a
    # matter of opinion. A few of the worthwhile lints in these categories are
    # reenabled in `DENY_LINTS` below.
    "clippy::style",
    "clippy::complexity",
    # clippy::large_enum_variant complains when enum variants have divergent
    # sizes, as the size of an enum is determined by the size of its largest
    # element. Obeying this lint is nearly always a premature optimization,
    # and the suggested solution—boxing the large variant—might actually result
    # in slower code due to the allocation.
    "clippy::large_enum_variant",
    # clippy::mutable_key_type disallows using internally mutable types as keys
    # in `HashMap`, because their order could change. This is a good lint in
    # principle, but its current implementation is too strict -- it disallows
    # anything containing an `Arc` or `Rc`, for example.
    "clippy::mutable_key_type",
    # Unstable sort is not strictly better than sort, notably on partially
    # sorted inputs.
    "clippy::stable_sort_primitive",
    # This lint has false positives where the pattern cannot be avoided without
    # cloning the key used in the map.
    "clippy::map_entry",
    # It is unclear if the performance gain from using `Box::default` instead of
    # `Box::new` is meaningful; and the lint can result in inconsistencies
    # when some types implement `Default` and others do not.
    # TODO(guswynn): benchmark the performance gain.
    "clippy::box_default",
]

WARN_LINTS = [
    # Comparison of a bool with `true` or `false` is indeed clearer as `x` or
    # `!x`.
    "clippy::bool_comparison",
    # Rewrite `x.clone()` to `Arc::clone(&x)`.
    # This clarifies a significant amount of code by making it visually clear
    # when a clone is cheap.
    "clippy::clone_on_ref_ptr",
    # These can catch real bugs, because something that is expected (a cast, a
    # conversion, a statement) is not happening.
    "clippy::no_effect",
    "clippy::unnecessary_unwrap",
    # Prevent code using the `dbg!` macro from being merged to the main branch.
    #
    # To mark a debugging print as intentional (e.g., in a test), use
    # `println!("{:?}", obj)` instead.
    "clippy::dbg_macro",
    # Prevent code containing the `todo!` macro from being merged to the main
    # branch.
    #
    # To mark something as intentionally unimplemented, use the `unimplemented!`
    # macro instead.
    "clippy::todo",
    # Wildcard dependencies are, by definition, incorrect. It is impossible
    # to be compatible with all future breaking changes in a crate.
    "clippy::wildcard_dependencies",
    # Zero-prefixed literals may be incorrectly interpreted as octal literals.
    "clippy::zero_prefixed_literal",
    # Purely redundant tokens.
    "clippy::borrowed_box",
    "clippy::deref_addrof",
    "clippy::double_must_use",
    "clippy::double_parens",
    "clippy::extra_unused_lifetimes",
    "clippy::needless_borrow",
    "clippy::needless_question_mark",
    "clippy::needless_return",
    "clippy::redundant_pattern",
    "clippy::redundant_slicing",
    "clippy::redundant_static_lifetimes",
    "clippy::single_component_path_imports",
    "clippy::unnecessary_cast",
    "clippy::useless_asref",
    "clippy::useless_conversion",
    # Very likely to be confusing.
    "clippy::builtin_type_shadow",
    "clippy::duplicate_underscore_argument",
    # Purely redundant tokens; very likely to be confusing.
    "clippy::double_neg",
    # Purely redundant tokens; code is misleading.
    "clippy::unnecessary_mut_passed",
    # Purely redundant tokens; probably a mistake.
    "clippy::wildcard_in_or_patterns",
    # Transmuting between T and T* seems 99% likely to be buggy code.
    "clippy::crosspointer_transmute",
    # Confusing and likely buggy.
    "clippy::excessive_precision",
    "clippy::overflow_check_conditional",
    # The `as` operator silently truncates data in many situations. It is very
    # difficult to assess whether a given usage of `as` is dangerous or not. So
    # ban it outright, to force usage of safer patterns, like `From` and
    # `TryFrom`.
    #
    # When absolutely essential (e.g., casting from a float to an integer), you
    # can attach `#[allow(clippy::as_conversion)]` to a single statement.
    "clippy::as_conversions",
    # Confusing.
    "clippy::match_overlapping_arm",
    # Confusing; possibly a mistake.
    "clippy::zero_divided_by_zero",
    # Probably a mistake.
    "clippy::must_use_unit",
    "clippy::suspicious_assignment_formatting",
    "clippy::suspicious_else_formatting",
    "clippy::suspicious_unary_op_formatting",
    # Legitimate performance impact.
    "clippy::mut_mutex_lock",
    "clippy::print_literal",
    "clippy::same_item_push",
    "clippy::useless_format",
    "clippy::write_literal",
    # Extra closures slow down compiles due to unnecessary code generation
    # that LLVM needs to optimize.
    "clippy::redundant_closure",
    "clippy::redundant_closure_call",
    "clippy::unnecessary_lazy_evaluations",
    # Provably either redundant or wrong.
    "clippy::partialeq_ne_impl",
    # This one is a debatable style nit, but it's so ingrained at this point
    # that we might as well keep it.
    "clippy::redundant_field_names",
    # Needless unsafe.
    "clippy::transmutes_expressible_as_ptr_casts",
    # Needless async.
    "clippy::unused_async",
    # Disallow the methods, macros, and types listed in clippy.toml;
    # see that file for rationale.
    "clippy::disallowed_methods",
    "clippy::disallowed_macros",
    "clippy::disallowed_types",
    # Implementing `From` gives you `Into` for free, but the reverse is not
    # true.
    "clippy::from_over_into",
]


def main() -> None:
    lint_config = [
        "// BEGIN LINT CONFIG\n",
        "// DO NOT EDIT. Automatically generated by bin/gen-lints.\n",
        "// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.\n",
        *(f"#![allow({lint})]\n" for lint in ALLOW_LINTS),
        *(f"#![warn({lint})]\n" for lint in WARN_LINTS),
        "// END LINT CONFIG\n",
    ]

    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version=1"]
        )
    )
    target_srcs = (
        Path(target["src_path"])
        for package in metadata["packages"]
        for target in package["targets"]
    )

    for src in target_srcs:
        contents = src.read_text().splitlines(keepends=True)
        try:
            # Overwrite existing lint configuration block.
            start = contents.index(lint_config[0])
            end = contents.index(lint_config[-1])
            contents[start : end + 1] = lint_config
        except ValueError:
            # No existing lint configuration block. Add a new one after the
            # copyright header.
            start = 0
            while start < len(contents) and contents[start].startswith("//"):
                start += 1
            contents[start : start + 1] = ["\n", *lint_config, "\n"]
        src.write_text("".join(contents))


if __name__ == "__main__":
    main()
