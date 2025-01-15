# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""The implementation of the mzbuild system for Docker images.

For an overview of what mzbuild is and why it exists, see the [user-facing
documentation][user-docs].

[user-docs]: https://github.com/MaterializeInc/materialize/blob/main/doc/developer/mzbuild.md
"""

import argparse
import base64
import collections
import hashlib
import json
import multiprocessing
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Sequence
from enum import Enum, auto
from functools import cache
from pathlib import Path
from tempfile import TemporaryFile
from typing import IO, Any, cast

import yaml

from materialize import bazel as bazel_utils
from materialize import cargo, git, rustc_flags, spawn, ui, xcompile
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch, target


class Fingerprint(bytes):
    """A SHA-1 hash of the inputs to an `Image`.

    The string representation uses base32 encoding to distinguish mzbuild
    fingerprints from Git's hex encoded SHA-1 hashes while still being
    URL safe.
    """

    def __str__(self) -> str:
        return base64.b32encode(self).decode()


class Profile(Enum):
    RELEASE = auto()
    OPTIMIZED = auto()
    DEV = auto()


class RepositoryDetails:
    """Immutable details about a `Repository`.

    Used internally by mzbuild.

    Attributes:
        root: The path to the root of the repository.
        arch: The CPU architecture to build for.
        profile: What profile the repository is being built with.
        coverage: Whether the repository has code coverage instrumentation
            enabled.
        sanitizer: Whether to use a sanitizer (address, hwaddress, cfi, thread, leak, memory, none)
        cargo_workspace: The `cargo.Workspace` associated with the repository.
        image_registry: The Docker image registry to pull images from and push
            images to.
        image_prefix: A prefix to apply to all Docker image names.
        bazel: Whether or not to use Bazel as the build system instead of Cargo.
        bazel_remote_cache: URL of a Bazel Remote Cache that we can build with.
    """

    def __init__(
        self,
        root: Path,
        arch: Arch,
        profile: Profile,
        coverage: bool,
        sanitizer: Sanitizer,
        image_registry: str,
        image_prefix: str,
        bazel: bool,
        bazel_remote_cache: str | None,
    ):
        self.root = root
        self.arch = arch
        self.profile = profile
        self.coverage = coverage
        self.sanitizer = sanitizer
        self.cargo_workspace = cargo.Workspace(root)
        self.image_registry = image_registry
        self.image_prefix = image_prefix
        self.bazel = bazel
        self.bazel_remote_cache = bazel_remote_cache

    def build(
        self,
        subcommand: str,
        rustflags: list[str],
        channel: str | None = None,
        extra_env: dict[str, str] = {},
    ) -> list[str]:
        """Start a build invocation for the configured architecture."""
        if self.bazel:
            assert not channel, "Bazel doesn't support building for multiple channels."
            return xcompile.bazel(
                arch=self.arch,
                subcommand=subcommand,
                rustflags=rustflags,
                extra_env=extra_env,
            )
        else:
            return xcompile.cargo(
                arch=self.arch,
                channel=channel,
                subcommand=subcommand,
                rustflags=rustflags,
                extra_env=extra_env,
            )

    def tool(self, name: str) -> list[str]:
        """Start a binutils tool invocation for the configured architecture."""
        if self.bazel:
            return ["bazel", "run", f"@//misc/bazel/tools:{name}", "--"]
        else:
            return xcompile.tool(self.arch, name)

    def cargo_target_dir(self) -> Path:
        """Determine the path to the target directory for Cargo."""
        return self.root / "target-xcompile" / xcompile.target(self.arch)

    def bazel_workspace_dir(self) -> Path:
        """Determine the path to the root of the Bazel workspace."""
        return self.root

    def bazel_config(self) -> list[str]:
        """Returns a set of Bazel config flags to set for the build."""
        flags = []

        if self.profile == Profile.RELEASE:
            # If we're a tagged build, then we'll use stamping to update our
            # build info, otherwise we'll use our side channel/best-effort
            # approach to update it.
            if ui.env_is_truthy("BUILDKITE_TAG"):
                flags.append("--config=release-tagged")
            else:
                flags.append("--config=release-dev")
                bazel_utils.write_git_hash()
        elif self.profile == Profile.OPTIMIZED:
            flags.append("--config=optimized")

        if self.bazel_remote_cache:
            flags.append(f"--remote_cache={self.bazel_remote_cache}")

        if ui.env_is_truthy("CI"):
            flags.append("--config=ci")

        return flags

    def rewrite_builder_path_for_host(self, path: Path) -> Path:
        """Rewrite a path that is relative to the target directory inside the
        builder to a path that is relative to the target directory on the host.

        If path does is not relative to the target directory inside the builder,
        it is returned unchanged.
        """
        builder_target_dir = Path("/mnt/build") / xcompile.target(self.arch)
        try:
            return self.cargo_target_dir() / path.relative_to(builder_target_dir)
        except ValueError:
            return path


def docker_images() -> set[str]:
    """List the Docker images available on the local machine."""
    return set(
        spawn.capture(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"])
        .strip()
        .split("\n")
    )


def is_docker_image_pushed(name: str) -> bool:
    """Check whether the named image is pushed to Docker Hub.

    Note that this operation requires a rather slow network request.
    """
    proc = subprocess.run(
        ["docker", "manifest", "inspect", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=dict(os.environ, DOCKER_CLI_EXPERIMENTAL="enabled"),
    )
    return proc.returncode == 0


def chmod_x(path: Path) -> None:
    """Set the executable bit on a file or directory."""
    # https://stackoverflow.com/a/30463972/1122351
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2  # copy R bits to X
    os.chmod(path, mode)


class PreImage:
    """An action to run before building a Docker image.

    Args:
        rd: The `RepositoryDetails` for the repository.
        path: The path to the `Image` associated with this action.
    """

    def __init__(self, rd: RepositoryDetails, path: Path):
        self.rd = rd
        self.path = path

    @classmethod
    def prepare_batch(cls, instances: list["PreImage"]) -> Any:
        """Prepare a batch of actions.

        This is useful for `PreImage` actions that are more efficient when
        their actions are applied to several images in bulk.

        Returns an arbitrary output that is passed to `PreImage.run`.
        """
        pass

    def run(self, prep: Any) -> None:
        """Perform the action.

        Args:
            prep: Any prep work returned by `prepare_batch`.
        """
        pass

    def inputs(self) -> set[str]:
        """Return the files which are considered inputs to the action."""
        raise NotImplementedError

    def extra(self) -> str:
        """Returns additional data for incorporation in the fingerprint."""
        return ""


class Copy(PreImage):
    """A `PreImage` action which copies files from a directory.

    See doc/developer/mzbuild.md for an explanation of the user-facing
    parameters.
    """

    def __init__(self, rd: RepositoryDetails, path: Path, config: dict[str, Any]):
        super().__init__(rd, path)

        self.source = config.pop("source", None)
        if self.source is None:
            raise ValueError("mzbuild config is missing 'source' argument")

        self.destination = config.pop("destination", None)
        if self.destination is None:
            raise ValueError("mzbuild config is missing 'destination' argument")

        self.matching = config.pop("matching", "*")

    def run(self, prep: Any) -> None:
        super().run(prep)
        for src in self.inputs():
            dst = self.path / self.destination / src
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(self.rd.root / self.source / src, dst)

    def inputs(self) -> set[str]:
        return set(git.expand_globs(self.rd.root / self.source, self.matching))


class CargoPreImage(PreImage):
    """A `PreImage` action that uses Cargo."""

    def inputs(self) -> set[str]:
        inputs = {
            "ci/builder",
            "Cargo.toml",
            # TODO(benesch): we could in theory fingerprint only the subset of
            # Cargo.lock that applies to the crates at hand, but that is a
            # *lot* of work.
            "Cargo.lock",
            ".cargo/config",
            # Even though we are not always building with Bazel, consider its
            # inputs so that developers with CI_BAZEL_BUILD=0 can still
            # download the images from Dockerhub
            ".bazelrc",
            "WORKSPACE",
        }

        # Bazel has some rules and additive files that aren't directly
        # associated with a crate, but can change how it's built.
        additive_path = self.rd.root / "misc" / "bazel"
        additive_files = ["BUILD.bazel", "*.bzl"]
        inputs |= set(git.expand_globs(additive_path, *additive_files))

        return inputs

    def extra(self) -> str:
        # Cargo images depend on the release mode and whether
        # coverage/sanitizer is enabled.
        flags: list[str] = []
        if self.rd.profile == Profile.RELEASE:
            flags += "release"
        if self.rd.profile == Profile.OPTIMIZED:
            flags += "optimized"
        if self.rd.coverage:
            flags += "coverage"
        if self.rd.sanitizer != Sanitizer.none:
            flags += self.rd.sanitizer.value
        flags.sort()
        return ",".join(flags)


class CargoBuild(CargoPreImage):
    """A `PreImage` action that builds a single binary with Cargo.

    See doc/developer/mzbuild.md for an explanation of the user-facing
    parameters.
    """

    def __init__(self, rd: RepositoryDetails, path: Path, config: dict[str, Any]):
        super().__init__(rd, path)
        bin = config.pop("bin", [])
        self.bins = bin if isinstance(bin, list) else [bin]
        example = config.pop("example", [])
        self.examples = example if isinstance(example, list) else [example]
        self.strip = config.pop("strip", True)
        self.extract = config.pop("extract", {})

        bazel_bins = config.pop("bazel-bin")
        self.bazel_bins = (
            bazel_bins if isinstance(bazel_bins, dict) else {self.bins[0]: bazel_bins}
        )
        self.bazel_tars = config.pop("bazel-tar", {})

        if len(self.bins) == 0 and len(self.examples) == 0:
            raise ValueError("mzbuild config is missing pre-build target")
        for bin in self.bins:
            if bin not in self.bazel_bins:
                raise ValueError(
                    f"need to specify a 'bazel-bin' for '{bin}' at '{path}'"
                )

    @staticmethod
    def generate_bazel_build_command(
        rd: RepositoryDetails,
        bins: list[str],
        examples: list[str],
        bazel_bins: dict[str, str],
        bazel_tars: dict[str, str],
    ) -> list[str]:
        assert (
            rd.bazel
        ), "Programming error, tried to invoke Bazel when it is not enabled."

        rustflags = []
        cflags = []

        if rd.coverage:
            assert (
                rd.sanitizer == Sanitizer.none
            ), "cannot get coverage and run a sanitizer at the same time"
            rustflags += rustc_flags.coverage
        elif rd.sanitizer != Sanitizer.none:
            rustflags += rustc_flags.sanitizer[rd.sanitizer]
            cflags += rustc_flags.sanitizer_cflags[rd.sanitizer]
        else:
            rustflags += ["--cfg=tokio_unstable"]

        extra_env = {
            "TSAN_OPTIONS": "report_bugs=0",  # build-scripts fail
        }

        bazel_build = rd.build(
            "build",
            channel=None,
            rustflags=rustflags,
            extra_env=extra_env,
        )

        for bin in bins:
            bazel_build.append(bazel_bins[bin])
        for tar in bazel_tars:
            bazel_build.append(tar)
        # TODO(parkmycar): Make sure cargo-gazelle generates rust_binary targets for examples.
        assert len(examples) == 0, "Bazel doesn't support building examples."

        # Add extra Bazel config flags.
        bazel_build.extend(rd.bazel_config())

        return bazel_build

    @staticmethod
    def generate_cargo_build_command(
        rd: RepositoryDetails,
        bins: list[str],
        examples: list[str],
    ) -> list[str]:
        assert (
            not rd.bazel
        ), "Programming error, tried to invoke Cargo when Bazel is enabled."

        rustflags = (
            rustc_flags.coverage
            if rd.coverage
            else (
                rustc_flags.sanitizer[rd.sanitizer]
                if rd.sanitizer != Sanitizer.none
                else ["--cfg=tokio_unstable"]
            )
        )
        cflags = (
            [
                f"--target={target(rd.arch)}",
                f"--gcc-toolchain=/opt/x-tools/{target(rd.arch)}/",
                "-fuse-ld=lld",
                f"--sysroot=/opt/x-tools/{target(rd.arch)}/{target(rd.arch)}/sysroot",
                f"-L/opt/x-tools/{target(rd.arch)}/{target(rd.arch)}/lib64",
            ]
            + rustc_flags.sanitizer_cflags[rd.sanitizer]
            if rd.sanitizer != Sanitizer.none
            else []
        )
        extra_env = (
            {
                "CFLAGS": " ".join(cflags),
                "CXXFLAGS": " ".join(cflags),
                "LDFLAGS": " ".join(cflags),
                "CXXSTDLIB": "stdc++",
                "CC": "cc",
                "CXX": "c++",
                "CPP": "clang-cpp-15",
                "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER": "cc",
                "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER": "cc",
                "PATH": f"/sanshim:/opt/x-tools/{target(rd.arch)}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "TSAN_OPTIONS": "report_bugs=0",  # build-scripts fail
            }
            if rd.sanitizer != Sanitizer.none
            else {}
        )

        cargo_build = rd.build(
            "build", channel=None, rustflags=rustflags, extra_env=extra_env
        )

        packages = set()
        for bin in bins:
            cargo_build.extend(["--bin", bin])
            packages.add(rd.cargo_workspace.crate_for_bin(bin).name)
        for example in examples:
            cargo_build.extend(["--example", example])
            packages.add(rd.cargo_workspace.crate_for_example(example).name)
        cargo_build.extend(f"--package={p}" for p in packages)

        if rd.profile == Profile.RELEASE:
            cargo_build.append("--release")
        if rd.profile == Profile.OPTIMIZED:
            cargo_build.extend(["--profile", "optimized"])
        if rd.sanitizer != Sanitizer.none:
            # ASan doesn't work with jemalloc
            cargo_build.append("--no-default-features")
            # Uses more memory, so reduce the number of jobs
            cargo_build.extend(
                ["--jobs", str(round(multiprocessing.cpu_count() * 2 / 3))]
            )

        return cargo_build

    @classmethod
    def prepare_batch(cls, cargo_builds: list["PreImage"]) -> dict[str, Any]:
        super().prepare_batch(cargo_builds)

        if not cargo_builds:
            return {}

        # Building all binaries and examples in the same `cargo build` command
        # allows Cargo to link in parallel with other work, which can
        # meaningfully speed up builds.

        rd: RepositoryDetails | None = None
        builds = cast(list[CargoBuild], cargo_builds)
        bins = set()
        examples = set()
        bazel_bins = dict()
        bazel_tars = dict()
        for build in builds:
            if not rd:
                rd = build.rd
            bins.update(build.bins)
            examples.update(build.examples)
            bazel_bins.update(build.bazel_bins)
            bazel_tars.update(build.bazel_tars)
        assert rd

        ui.section(f"Common build for: {', '.join(bins | examples)}")

        if rd.bazel:
            cargo_build = cls.generate_bazel_build_command(
                rd, list(bins), list(examples), bazel_bins, bazel_tars
            )
        else:
            cargo_build = cls.generate_cargo_build_command(
                rd, list(bins), list(examples)
            )

        spawn.runv(cargo_build, cwd=rd.root)

        # Re-run with JSON-formatted messages and capture the output so we can
        # later analyze the build artifacts in `run`. This should be nearly
        # instantaneous since we just compiled above with the same crates and
        # features. (We don't want to do the compile above with JSON-formatted
        # messages because it wouldn't be human readable.)
        if rd.bazel:
            # TODO(parkmycar): Having to assign the same compilation flags as the build process
            # is a bit brittle. It would be better if the Bazel build process itself could
            # output the file to a known location.
            options = rd.bazel_config()
            paths_to_binaries = {}
            for bin in bins:
                paths = bazel_utils.output_paths(bazel_bins[bin], options)
                assert len(paths) == 1, f"{bazel_bins[bin]} output more than 1 file"
                paths_to_binaries[bin] = paths[0]
            for tar in bazel_tars:
                paths = bazel_utils.output_paths(tar, options)
                assert len(paths) == 1, f"more than one output path found for '{tar}'"
                paths_to_binaries[tar] = paths[0]
            prep = {"bazel": paths_to_binaries}
        else:
            json_output = spawn.capture(
                cargo_build + ["--message-format=json"],
                cwd=rd.root,
            )
            prep = {"cargo": json_output}

        return prep

    def build(self, build_output: dict[str, Any]) -> None:
        cargo_profile = (
            "release"
            if self.rd.profile == Profile.RELEASE
            else "optimized" if self.rd.profile == Profile.OPTIMIZED else "debug"
        )

        def copy(src: Path, relative_dst: Path) -> None:
            exe_path = self.path / relative_dst
            exe_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, exe_path)

            # Bazel doesn't add write or exec permissions for built binaries
            # but `strip` and `objcopy` need write permissions and we add exec
            # permissions for the built Docker images.
            current_perms = os.stat(exe_path).st_mode
            new_perms = (
                current_perms
                # chmod +wx
                | stat.S_IWUSR
                | stat.S_IWGRP
                | stat.S_IWOTH
                | stat.S_IXUSR
                | stat.S_IXGRP
                | stat.S_IXOTH
            )
            os.chmod(exe_path, new_perms)

            if self.strip:
                # The debug information is large enough that it slows down CI,
                # since we're packaging these binaries up into Docker images and
                # shipping them around.
                spawn.runv(
                    [*self.rd.tool("strip"), "--strip-debug", exe_path],
                    cwd=self.rd.root,
                )
            else:
                # Even if we've been asked not to strip the binary, remove the
                # `.debug_pubnames` and `.debug_pubtypes` sections. These are just
                # indexes that speed up launching a debugger against the binary,
                # and we're happy to have slower debugger start up in exchange for
                # smaller binaries. Plus the sections have been obsoleted by a
                # `.debug_names` section in DWARF 5, and so debugger support for
                # `.debug_pubnames`/`.debug_pubtypes` is minimal anyway.
                # See: https://github.com/rust-lang/rust/issues/46034
                spawn.runv(
                    [
                        *self.rd.tool("objcopy"),
                        "-R",
                        ".debug_pubnames",
                        "-R",
                        ".debug_pubtypes",
                        exe_path,
                    ],
                    cwd=self.rd.root,
                )

        for bin in self.bins:
            if "bazel" in build_output:
                src_path = self.rd.bazel_workspace_dir() / build_output["bazel"][bin]
            else:
                src_path = self.rd.cargo_target_dir() / cargo_profile / bin
            copy(src_path, bin)
        for example in self.examples:
            src_path = (
                self.rd.cargo_target_dir() / cargo_profile / Path("examples") / example
            )
            copy(src_path, Path("examples") / example)

        # Bazel doesn't support 'extract', instead you need to use 'bazel-tar'
        if self.extract and "bazel" not in build_output:
            cargo_build_json_output = build_output["cargo"]

            target_dir = self.rd.cargo_target_dir()
            for line in cargo_build_json_output.split("\n"):
                if line.strip() == "" or not line.startswith("{"):
                    continue
                message = json.loads(line)
                if message["reason"] != "build-script-executed":
                    continue
                out_dir = self.rd.rewrite_builder_path_for_host(
                    Path(message["out_dir"])
                )
                if not out_dir.is_relative_to(target_dir):
                    # Some crates are built for both the host and the target.
                    # Ignore the built-for-host out dir.
                    continue
                # parse the package name from a package_id that looks like one of:
                # git+https://github.com/MaterializeInc/rust-server-sdk#launchdarkly-server-sdk@1.0.0
                # path+file:///Users/roshan/materialize/src/catalog#mz-catalog@0.0.0
                # registry+https://github.com/rust-lang/crates.io-index#num-rational@0.4.0
                # file:///path/to/my-package#0.1.0
                package_id = message["package_id"]
                if "@" in package_id:
                    package = package_id.split("@")[0].split("#")[-1]
                else:
                    package = message["package_id"].split("#")[0].split("/")[-1]
                for src, dst in self.extract.get(package, {}).items():
                    spawn.runv(["cp", "-R", out_dir / src, self.path / dst])

        if self.bazel_tars and "bazel" in build_output:
            ui.section("Extracing 'bazel-tar'")
            for tar in self.bazel_tars:
                # Where Bazel built the tarball.
                tar_path = self.rd.bazel_workspace_dir() / build_output["bazel"][tar]
                # Where we need to extract it into.
                tar_dest = self.path / self.bazel_tars[tar]
                ui.say(f"extracing {tar_path} to {tar_dest}")

                with tarfile.open(tar_path, "r") as tar_file:
                    os.makedirs(tar_dest, exist_ok=True)
                    tar_file.extractall(path=tar_dest)

        self.acquired = True

    def run(self, prep: dict[str, Any]) -> None:
        super().run(prep)
        self.build(prep)

    def inputs(self) -> set[str]:
        deps = set()

        for bin in self.bins:
            crate = self.rd.cargo_workspace.crate_for_bin(bin)
            deps |= self.rd.cargo_workspace.transitive_path_dependencies(crate)
        for example in self.examples:
            crate = self.rd.cargo_workspace.crate_for_example(example)
            deps |= self.rd.cargo_workspace.transitive_path_dependencies(
                crate, dev=True
            )
        inputs = super().inputs() | set(inp for dep in deps for inp in dep.inputs())
        # Even though we are not always building with Bazel, consider its
        # inputs so that developers with CI_BAZEL_BUILD=0 can still
        # download the images from Dockerhub
        inputs |= {"BUILD.bazel"}

        return inputs


class Image:
    """A Docker image whose build and dependencies are managed by mzbuild.

    An image corresponds to a directory in a repository that contains a
    `mzbuild.yml` file. This directory is called an "mzbuild context."

    Attributes:
        name: The name of the image.
        publish: Whether the image should be pushed to Docker Hub.
        depends_on: The names of the images upon which this image depends.
        root: The path to the root of the associated `Repository`.
        path: The path to the directory containing the `mzbuild.yml`
            configuration file.
        pre_images: Optional actions to perform before running `docker build`.
        build_args: An optional list of --build-arg to pass to the dockerfile
    """

    _DOCKERFILE_MZFROM_RE = re.compile(rb"^MZFROM\s*(\S+)")

    def __init__(self, rd: RepositoryDetails, path: Path):
        self.rd = rd
        self.path = path
        self.pre_images: list[PreImage] = []
        with open(self.path / "mzbuild.yml") as f:
            data = yaml.safe_load(f)
            self.name: str = data.pop("name")
            self.publish: bool = data.pop("publish", True)
            self.description: str | None = data.pop("description", None)
            self.mainline: bool = data.pop("mainline", True)
            for pre_image in data.pop("pre-image", []):
                typ = pre_image.pop("type", None)
                if typ == "cargo-build":
                    self.pre_images.append(CargoBuild(self.rd, self.path, pre_image))
                elif typ == "copy":
                    self.pre_images.append(Copy(self.rd, self.path, pre_image))
                else:
                    raise ValueError(
                        f"mzbuild config in {self.path} has unknown pre-image type"
                    )
            self.build_args = data.pop("build-args", {})

        if re.search(r"[^A-Za-z0-9\-]", self.name):
            raise ValueError(
                f"mzbuild image name {self.name} contains invalid character; only alphanumerics and hyphens allowed"
            )

        self.depends_on: list[str] = []
        with open(self.path / "Dockerfile", "rb") as f:
            for line in f:
                match = self._DOCKERFILE_MZFROM_RE.match(line)
                if match:
                    self.depends_on.append(match.group(1).decode())

    def sync_description(self) -> None:
        """Sync the description to Docker Hub if the image is publishable
        and a README.md file exists."""

        if not self.publish:
            ui.say(f"{self.name} is not publishable")
            return

        readme_path = self.path / "README.md"
        has_readme = readme_path.exists()
        if not has_readme:
            ui.say(f"{self.name} has no README.md or description")
            return

        docker_config = os.getenv("DOCKER_CONFIG")
        spawn.runv(
            [
                "docker",
                "pushrm",
                f"--file={readme_path}",
                *([f"--config={docker_config}/config.json"] if docker_config else []),
                *([f"--short={self.description}"] if self.description else []),
                self.docker_name(),
            ]
        )

    def docker_name(self, tag: str | None = None) -> str:
        """Return the name of the image on Docker Hub at the given tag."""
        name = f"{self.rd.image_registry}/{self.rd.image_prefix}{self.name}"
        if tag:
            name += f":{tag}"
        return name


class ResolvedImage:
    """An `Image` whose dependencies have been resolved.

    Attributes:
        image: The underlying `Image`.
        acquired: Whether the image is available locally.
        dependencies: A mapping from dependency name to `ResolvedImage` for
            each of the images that `image` depends upon.
    """

    def __init__(self, image: Image, dependencies: Iterable["ResolvedImage"]):
        self.image = image
        self.acquired = False
        self.dependencies = {}
        for d in dependencies:
            self.dependencies[d.name] = d

    def __repr__(self) -> str:
        return f"ResolvedImage<{self.spec()}>"

    @property
    def name(self) -> str:
        """The name of the underlying image."""
        return self.image.name

    @property
    def publish(self) -> bool:
        """Whether the underlying image should be pushed to Docker Hub."""
        return self.image.publish

    def spec(self) -> str:
        """Return the "spec" for the image.

        A spec is the unique identifier for the image given its current
        fingerprint. It is a valid Docker Hub name.
        """
        return self.image.docker_name(tag=f"mzbuild-{self.fingerprint()}")

    def write_dockerfile(self) -> IO[bytes]:
        """Render the Dockerfile without mzbuild directives.

        Returns:
            file: A handle to a temporary file containing the adjusted
                Dockerfile."""
        with open(self.image.path / "Dockerfile", "rb") as f:
            lines = f.readlines()
        f = TemporaryFile()
        for line in lines:
            match = Image._DOCKERFILE_MZFROM_RE.match(line)
            if match:
                image = match.group(1).decode()
                spec = self.dependencies[image].spec()
                line = Image._DOCKERFILE_MZFROM_RE.sub(b"FROM %b" % spec.encode(), line)
            f.write(line)
        f.seek(0)
        return f

    def build(self, prep: dict[type[PreImage], Any]) -> None:
        """Build the image from source.

        Requires that the caller has already acquired all dependencies and
        prepared all `PreImage` actions via `PreImage.prepare_batch`.
        """
        ui.section(f"Building {self.spec()}")
        spawn.runv(["git", "clean", "-ffdX", self.image.path])

        for pre_image in self.image.pre_images:
            pre_image.run(prep[type(pre_image)])
        build_args = {
            **self.image.build_args,
            "ARCH_GCC": str(self.image.rd.arch),
            "ARCH_GO": self.image.rd.arch.go_str(),
            "CI_SANITIZER": str(self.image.rd.sanitizer),
        }
        f = self.write_dockerfile()
        cmd: Sequence[str] = [
            "docker",
            "build",
            "-f",
            "-",
            *(f"--build-arg={k}={v}" for k, v in build_args.items()),
            "-t",
            self.spec(),
            f"--platform=linux/{self.image.rd.arch.go_str()}",
            str(self.image.path),
        ]
        spawn.runv(cmd, stdin=f, stdout=sys.stderr.buffer)

    def try_pull(self, max_retries: int) -> bool:
        """Download the image if it does not exist locally. Returns whether it was found."""
        ui.header(f"Acquiring {self.spec()}")
        command = ["docker", "pull"]
        # --quiet skips printing the progress bar, which does not display well in CI.
        if ui.env_is_truthy("CI"):
            command.append("--quiet")
        command.append(self.spec())
        if not self.acquired:
            sleep_time = 1
            for retry in range(1, max_retries + 1):
                try:
                    spawn.runv(
                        command,
                        stdout=sys.stderr.buffer,
                    )
                    self.acquired = True
                except subprocess.CalledProcessError:
                    if retry < max_retries:
                        # There seems to be no good way to tell what error
                        # happened based on error code
                        # (https://github.com/docker/cli/issues/538) and we
                        # want to print output directly to terminal.
                        print(f"Retrying in {sleep_time}s ...")
                        time.sleep(sleep_time)
                        sleep_time *= 2
                        continue
                    else:
                        break
        return self.acquired

    def is_published_if_necessary(self) -> bool:
        """Report whether the image exists on Docker Hub if it is publishable."""
        if self.publish and is_docker_image_pushed(self.spec()):
            ui.say(f"{self.spec()} already exists")
            return True
        return False

    def run(
        self,
        args: list[str] = [],
        docker_args: list[str] = [],
        env: dict[str, str] = {},
    ) -> None:
        """Run a command in the image.

        Creates a container from the image and runs the command described by
        `args` in the image.
        """
        envs = []
        for key, val in env.items():
            envs.extend(["--env", f"{key}={val}"])
        spawn.runv(
            [
                "docker",
                "run",
                "--tty",
                "--rm",
                *envs,
                "--init",
                *docker_args,
                self.spec(),
                *args,
            ],
        )

    def list_dependencies(self, transitive: bool = False) -> set[str]:
        out = set()
        for dep in self.dependencies.values():
            out.add(dep.name)
            if transitive:
                out |= dep.list_dependencies(transitive)
        return out

    def inputs(self, transitive: bool = False) -> set[str]:
        """List the files tracked as inputs to the image.

        These files are used to compute the fingerprint for the image. See
        `ResolvedImage.fingerprint` for details.

        Returns:
            inputs: A list of input files, relative to the root of the
                repository.
        """
        paths = set(git.expand_globs(self.image.rd.root, f"{self.image.path}/**"))
        if not paths:
            # While we could find an `mzbuild.yml` file for this service, expland_globs didn't
            # return any files that matched this service. At the very least, the `mzbuild.yml`
            # file itself should have been returned. We have a bug if paths is empty.
            raise AssertionError(
                f"{self.image.name} mzbuild exists but its files are unknown to git"
            )
        for pre_image in self.image.pre_images:
            paths |= pre_image.inputs()
        if transitive:
            for dep in self.dependencies.values():
                paths |= dep.inputs(transitive)
        return paths

    @cache
    def fingerprint(self) -> Fingerprint:
        """Fingerprint the inputs to the image.

        Compute the fingerprint of the image. Changing the contents of any of
        the files or adding or removing files to the image will change the
        fingerprint, as will modifying the inputs to any of its dependencies.

        The image considers all non-gitignored files in its mzbuild context to
        be inputs. If it has a pre-image action, that action may add additional
        inputs via `PreImage.inputs`.
        """
        self_hash = hashlib.sha1()
        for rel_path in sorted(
            set(git.expand_globs(self.image.rd.root, *self.inputs()))
        ):
            abs_path = self.image.rd.root / rel_path
            file_hash = hashlib.sha1()
            raw_file_mode = os.lstat(abs_path).st_mode
            # Compute a simplified file mode using the same rules as Git.
            # https://github.com/git/git/blob/3bab5d562/Documentation/git-fast-import.txt#L610-L616
            if stat.S_ISLNK(raw_file_mode):
                file_mode = 0o120000
            elif raw_file_mode & stat.S_IXUSR:
                file_mode = 0o100755
            else:
                file_mode = 0o100644
            with open(abs_path, "rb") as f:
                file_hash.update(f.read())
            self_hash.update(file_mode.to_bytes(2, byteorder="big"))
            self_hash.update(rel_path.encode())
            self_hash.update(file_hash.digest())
            self_hash.update(b"\0")

        for pre_image in self.image.pre_images:
            self_hash.update(pre_image.extra().encode())
            self_hash.update(b"\0")

        self_hash.update(f"arch={self.image.rd.arch}".encode())
        self_hash.update(f"coverage={self.image.rd.coverage}".encode())
        self_hash.update(f"sanitizer={self.image.rd.sanitizer}".encode())

        full_hash = hashlib.sha1()
        full_hash.update(self_hash.digest())
        for dep in sorted(self.dependencies.values(), key=lambda d: d.name):
            full_hash.update(dep.name.encode())
            full_hash.update(dep.fingerprint())
            full_hash.update(b"\0")

        return Fingerprint(full_hash.digest())


class DependencySet:
    """A set of `ResolvedImage`s.

    Iterating over a dependency set yields the contained images in an arbitrary
    order. Indexing a dependency set yields the image with the specified name.
    """

    def __init__(self, dependencies: Iterable[Image]):
        """Construct a new `DependencySet`.

        The provided `dependencies` must be topologically sorted.
        """
        self._dependencies: dict[str, ResolvedImage] = {}
        known_images = docker_images()
        for d in dependencies:
            image = ResolvedImage(
                image=d,
                dependencies=(self._dependencies[d0] for d0 in d.depends_on),
            )
            image.acquired = image.spec() in known_images
            self._dependencies[d.name] = image

    def _prepare_batch(self, images: list[ResolvedImage]) -> dict[type[PreImage], Any]:
        pre_images = collections.defaultdict(list)
        for image in images:
            for pre_image in image.image.pre_images:
                pre_images[type(pre_image)].append(pre_image)
        pre_image_prep = {}
        for cls, instances in pre_images.items():
            pre_image = cast(PreImage, cls)
            pre_image_prep[cls] = pre_image.prepare_batch(instances)
        return pre_image_prep

    def acquire(self, max_retries: int | None = None) -> None:
        """Download or build all of the images in the dependency set that do not
        already exist locally.

        Args:
            max_retries: Number of retries on failure.
        """

        # Only retry in CI runs since we struggle with flaky docker pulls there
        if not max_retries:
            max_retries = 5 if ui.env_is_truthy("CI") else 1
        assert max_retries > 0

        deps_to_build = [
            dep for dep in self if not dep.publish or not dep.try_pull(max_retries)
        ]

        # Don't attempt to build in CI, as our timeouts and small machines won't allow it anyway
        if ui.env_is_truthy("CI"):
            expected_deps = [dep for dep in deps_to_build if dep.publish]
            if expected_deps:
                print(
                    f"+++ Expected builds to be available, the build probably failed, so not proceeding: {expected_deps}"
                )
                sys.exit(5)

        prep = self._prepare_batch(deps_to_build)
        for dep in deps_to_build:
            dep.build(prep)

    def ensure(self, post_build: Callable[[ResolvedImage], None] | None = None):
        """Ensure all publishable images in this dependency set exist on Docker
        Hub.

        Images are pushed using their spec as their tag.

        Args:
            post_build: A callback to invoke with each dependency that was built
                locally.
        """
        deps_to_build = [dep for dep in self if not dep.is_published_if_necessary()]
        prep = self._prepare_batch(deps_to_build)

        images_to_push = []
        for dep in deps_to_build:
            dep.build(prep)
            if post_build:
                post_build(dep)
            if dep.publish:
                images_to_push.append(dep.spec())

        # Push all Docker images in parallel to minimize build time.
        ui.section("Pushing images")
        # Attempt to upload images a maximum of 3 times before giving up.
        for attempts_remaining in reversed(range(3)):
            pushes: list[subprocess.Popen] = []
            for image in images_to_push:
                # Piping through `cat` disables terminal control codes, and so the
                # interleaved progress output from multiple pushes is less hectic.
                # We don't use `docker push --quiet`, as that disables progress
                # output entirely. Use `set -o pipefail` so the return code of
                # `docker push` is passed through.
                push = subprocess.Popen(
                    [
                        "/bin/bash",
                        "-c",
                        f"set -o pipefail; docker push {shlex.quote(image)} | cat",
                    ]
                )
                pushes.append(push)

            for i, push in reversed(list(enumerate(pushes))):
                returncode = push.wait()
                if returncode:
                    if attempts_remaining == 0:
                        # Last attempt, fail
                        raise subprocess.CalledProcessError(returncode, push.args)
                    else:
                        print(f"docker push {push.args} failed: {returncode}")
                else:
                    del images_to_push[i]

            if images_to_push:
                time.sleep(10)
                print("Retrying in 10 seconds")

    def check(self) -> bool:
        """Check all publishable images in this dependency set exist on Docker
        Hub. Don't try to download or build them."""
        return all(dep.is_published_if_necessary() for dep in self)

    def __iter__(self) -> Iterator[ResolvedImage]:
        return iter(self._dependencies.values())

    def __getitem__(self, key: str) -> ResolvedImage:
        return self._dependencies[key]


class Repository:
    """A collection of mzbuild `Image`s.

    Creating a repository will walk the filesystem beneath `root` to
    automatically discover all contained `Image`s.

    Iterating over a repository yields the contained images in an arbitrary
    order.

    Args:
        root: The path to the root of the repository.
        arch: The CPU architecture to build for.
        profile: What profile to build the repository in.
        coverage: Whether to enable code coverage instrumentation.
        sanitizer: Whether to a sanitizer (address, thread, leak, memory, none)
        image_registry: The Docker image registry to pull images from and push
            images to.
        image_prefix: A prefix to apply to all Docker image names.

    Attributes:
        images: A mapping from image name to `Image` for all contained images.
        compose_dirs: The set of directories containing a `mzcompose.py` file.
    """

    def __init__(
        self,
        root: Path,
        arch: Arch = Arch.host(),
        profile: Profile = Profile.RELEASE,
        coverage: bool = False,
        sanitizer: Sanitizer = Sanitizer.none,
        image_registry: str = "materialize",
        image_prefix: str = "",
        bazel: bool = False,
        bazel_remote_cache: str | None = None,
    ):
        self.rd = RepositoryDetails(
            root,
            arch,
            profile,
            coverage,
            sanitizer,
            image_registry,
            image_prefix,
            bazel,
            bazel_remote_cache,
        )
        self.images: dict[str, Image] = {}
        self.compositions: dict[str, Path] = {}
        for path, dirs, files in os.walk(self.root, topdown=True):
            if path == str(root / "misc"):
                dirs.remove("python")
            # Filter out some particularly massive ignored directories to keep
            # things snappy. Not required for correctness.
            dirs[:] = set(dirs) - {
                ".git",
                ".mypy_cache",
                "target",
                "target-ra",
                "target-xcompile",
                "mzdata",
                "node_modules",
                "venv",
            }
            if "mzbuild.yml" in files:
                image = Image(self.rd, Path(path))
                if not image.name:
                    raise ValueError(f"config at {path} missing name")
                if image.name in self.images:
                    raise ValueError(f"image {image.name} exists twice")
                self.images[image.name] = image
            if "mzcompose.py" in files:
                name = Path(path).name
                if name in self.compositions:
                    raise ValueError(f"composition {name} exists twice")
                self.compositions[name] = Path(path)

        # Validate dependencies.
        for image in self.images.values():
            for d in image.depends_on:
                if d not in self.images:
                    raise ValueError(
                        f"image {image.name} depends on non-existent image {d}"
                    )

    @staticmethod
    def install_arguments(parser: argparse.ArgumentParser) -> None:
        """Install options to configure a repository into an argparse parser.

        This function installs the following options:

          * The mutually-exclusive `--dev`/`--optimized`/`--release` options to control the
            `profile` repository attribute.
          * The `--coverage` boolean option to control the `coverage` repository
            attribute.

        Use `Repository.from_arguments` to construct a repository from the
        parsed command-line arguments.
        """
        build_mode = parser.add_mutually_exclusive_group()
        build_mode.add_argument(
            "--dev",
            action="store_true",
            help="build Rust binaries with the dev profile",
        )
        build_mode.add_argument(
            "--release",
            action="store_true",
            help="build Rust binaries with the release profile (default)",
        )
        build_mode.add_argument(
            "--optimized",
            action="store_true",
            help="build Rust binaries with the optimized profile (optimizations, no LTO, no debug symbols)",
        )
        parser.add_argument(
            "--coverage",
            help="whether to enable code coverage compilation flags",
            default=ui.env_is_truthy("CI_COVERAGE_ENABLED"),
            action="store_true",
        )
        parser.add_argument(
            "--sanitizer",
            help="whether to enable a sanitizer",
            default=Sanitizer[os.getenv("CI_SANITIZER", "none")],
            type=Sanitizer,
            choices=Sanitizer,
        )
        parser.add_argument(
            "--arch",
            default=Arch.host(),
            help="the CPU architecture to build for",
            type=Arch,
            choices=Arch,
        )
        parser.add_argument(
            "--image-registry",
            default="materialize",
            help="the Docker image registry to pull images from and push images to",
        )
        parser.add_argument(
            "--image-prefix",
            default="",
            help="a prefix to apply to all Docker image names",
        )
        parser.add_argument(
            "--bazel",
            default=ui.env_is_truthy("CI_BAZEL_BUILD"),
            action="store_true",
        )
        parser.add_argument(
            "--bazel-remote-cache",
            default=os.getenv("CI_BAZEL_REMOTE_CACHE"),
            action="store",
        )

    @classmethod
    def from_arguments(cls, root: Path, args: argparse.Namespace) -> "Repository":
        """Construct a repository from command-line arguments.

        The provided namespace must contain the options installed by
        `Repository.install_arguments`.
        """
        if args.release:
            profile = Profile.RELEASE
        elif args.optimized:
            profile = Profile.OPTIMIZED
        elif args.dev:
            profile = Profile.DEV
        else:
            profile = Profile.RELEASE

        return cls(
            root,
            profile=profile,
            coverage=args.coverage,
            sanitizer=args.sanitizer,
            image_registry=args.image_registry,
            image_prefix=args.image_prefix,
            arch=args.arch,
            bazel=args.bazel,
            bazel_remote_cache=args.bazel_remote_cache,
        )

    @property
    def root(self) -> Path:
        """The path to the root directory for the repository."""
        return self.rd.root

    def resolve_dependencies(self, targets: Iterable[Image]) -> DependencySet:
        """Compute the dependency set necessary to build target images.

        The dependencies of `targets` will be crawled recursively until the
        complete set of transitive dependencies is determined or a circular
        dependency is discovered. The returned dependency set will be sorted
        in topological order.

        Raises:
           ValueError: A circular dependency was discovered in the images
               in the repository.
        """
        resolved = OrderedDict()
        visiting = set()

        def visit(image: Image, path: list[str] = []) -> None:
            if image.name in resolved:
                return
            if image.name in visiting:
                diagram = " -> ".join(path + [image.name])
                raise ValueError(f"circular dependency in mzbuild: {diagram}")

            visiting.add(image.name)
            for d in sorted(image.depends_on):
                visit(self.images[d], path + [image.name])
            resolved[image.name] = image

        for target_image in sorted(targets, key=lambda image: image.name):
            visit(target_image)

        return DependencySet(resolved.values())

    def __iter__(self) -> Iterator[Image]:
        return iter(self.images.values())


def publish_multiarch_images(
    tag: str, dependency_sets: Iterable[Iterable[ResolvedImage]]
) -> None:
    """Publishes a set of docker images under a given tag."""
    for images in zip(*dependency_sets):
        names = set(image.image.name for image in images)
        assert len(names) == 1, "dependency sets did not contain identical images"
        name = images[0].image.docker_name(tag)
        spawn.runv(
            ["docker", "manifest", "create", name, *(image.spec() for image in images)]
        )
        spawn.runv(["docker", "manifest", "push", name])
    print(f"--- Nofifying for tag {tag}")
    markdown = f"""Pushed images with Docker tag `{tag}`"""
    spawn.runv(
        [
            "buildkite-agent",
            "annotate",
            "--style=info",
            f"--context=build-tags-{tag}",
        ],
        stdin=markdown.encode(),
    )
