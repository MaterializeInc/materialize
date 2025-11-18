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
import io
import json
import multiprocessing
import os
import platform
import re
import selectors
import shutil
import stat
import subprocess
import sys
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum, auto
from functools import cache
from pathlib import Path
from tempfile import TemporaryFile
from threading import Lock
from typing import IO, Any, cast

import requests
import yaml
from requests.auth import HTTPBasicAuth

from materialize import MZ_ROOT, buildkite, cargo, git, rustc_flags, spawn, ui, xcompile
from materialize.docker import MZ_GHCR_DEFAULT
from materialize.rustc_flags import Sanitizer
from materialize.xcompile import Arch, target


class RustICE(Exception):
    pass


def run_and_detect_rust_ice(
    cmd: list[str], cwd: str | Path
) -> subprocess.CompletedProcess:
    """This function is complex since it prints out each line immediately to
    stdout/stderr, but still records them at the same time so that we can scan
    for the Rust ICE."""
    stdout_result = io.StringIO()
    stderr_result = io.StringIO()
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env={**os.environ, "CARGO_TERM_COLOR": "always", "RUSTC_COLOR": "always"},
    )

    sel = selectors.DefaultSelector()
    sel.register(p.stdout, selectors.EVENT_READ)  # type: ignore
    sel.register(p.stderr, selectors.EVENT_READ)  # type: ignore
    assert p.stdout is not None
    assert p.stderr is not None
    os.set_blocking(p.stdout.fileno(), False)
    os.set_blocking(p.stderr.fileno(), False)
    running = True
    while running:
        for key, val in sel.select():
            output = io.StringIO()
            running = False
            while True:
                new_output = key.fileobj.read(1024)  # type: ignore
                if not new_output:
                    break
                output.write(new_output)
            contents = output.getvalue()
            output.close()
            if not contents:
                continue
            # Keep running as long as stdout or stderr have any content
            running = True
            if key.fileobj is p.stdout:
                print(
                    contents,
                    end="",
                    flush=True,
                )
                stdout_result.write(contents)
            else:
                print(
                    contents,
                    end="",
                    file=sys.stderr,
                    flush=True,
                )
                stderr_result.write(contents)
    p.wait()
    retcode = p.poll()
    assert retcode is not None
    stdout_contents = stdout_result.getvalue()
    stdout_result.close()
    stderr_contents = stderr_result.getvalue()
    stderr_result.close()
    if retcode:
        panic_msg = "panicked at compiler/rustc_metadata/src/rmeta/def_path_hash_map.rs"
        if panic_msg in stdout_contents or panic_msg in stderr_contents:
            raise RustICE()

        raise subprocess.CalledProcessError(
            retcode, p.args, output=stdout_contents, stderr=stderr_contents
        )
    return subprocess.CompletedProcess(
        p.args, retcode, stdout_contents, stderr_contents
    )


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
    ):
        self.root = root
        self.arch = arch
        self.profile = profile
        self.coverage = coverage
        self.sanitizer = sanitizer
        self.cargo_workspace = cargo.Workspace(root)
        self.image_registry = image_registry
        self.image_prefix = image_prefix

    def build(
        self,
        subcommand: str,
        rustflags: list[str],
        channel: str | None = None,
        extra_env: dict[str, str] = {},
    ) -> list[str]:
        """Start a build invocation for the configured architecture."""
        return xcompile.cargo(
            arch=self.arch,
            channel=channel,
            subcommand=subcommand,
            rustflags=rustflags,
            extra_env=extra_env,
        )

    def tool(self, name: str) -> list[str]:
        """Start a binutils tool invocation for the configured architecture."""
        if platform.system() != "Linux":
            # We can't use the local tools from macOS to build a Linux executable
            return ["bin/ci-builder", "run", "stable", name]
        # If we're on Linux, trust that the tools are installed instead of
        # loading the slow ci-builder. If you don't have compilation tools
        # installed you can still run `bin/ci-builder run stable
        # bin/mzcompose ...`, and most likely the Cargo build will already
        # fail earlier if you don't have compilation tools installed and
        # run without the ci-builder.
        return [name]

    def cargo_target_dir(self) -> Path:
        """Determine the path to the target directory for Cargo."""
        return self.root / "target-xcompile" / xcompile.target(self.arch)

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


KNOWN_DOCKER_IMAGES_FILE = Path(MZ_ROOT / "known-docker-images.txt")
_known_docker_images: set[str] | None = None
_known_docker_images_lock = Lock()


def is_docker_image_pushed(name: str) -> bool:
    """Check whether the named image is pushed to Docker Hub.

    Note that this operation requires a rather slow network request.
    """
    global _known_docker_images

    if _known_docker_images is None:
        with _known_docker_images_lock:
            if not KNOWN_DOCKER_IMAGES_FILE.exists():
                _known_docker_images = set()
            else:
                with KNOWN_DOCKER_IMAGES_FILE.open() as f:
                    _known_docker_images = set(line.strip() for line in f)

    if name in _known_docker_images:
        return True

    if ":" not in name:
        image, tag = name, "latest"
    else:
        image, tag = name.rsplit(":", 1)

    dockerhub_username = os.getenv("DOCKERHUB_USERNAME")
    dockerhub_token = os.getenv("DOCKERHUB_ACCESS_TOKEN")

    exists: bool = False

    try:
        if dockerhub_username and dockerhub_token:
            response = requests.head(
                f"https://registry-1.docker.io/v2/{image}/manifests/{tag}",
                headers={
                    "Accept": "application/vnd.docker.distribution.manifest.v2+json",
                },
                auth=HTTPBasicAuth(dockerhub_username, dockerhub_token),
            )
        else:
            token = requests.get(
                "https://auth.docker.io/token",
                params={
                    "service": "registry.docker.io",
                    "scope": f"repository:{image}:pull",
                },
            ).json()["token"]
            response = requests.head(
                f"https://registry-1.docker.io/v2/{image}/manifests/{tag}",
                headers={
                    "Accept": "application/vnd.docker.distribution.manifest.v2+json",
                    "Authorization": f"Bearer {token}",
                },
            )

        if response.status_code in (401, 429, 500, 502, 503, 504):
            # Fall back to 5x slower method
            proc = subprocess.run(
                ["docker", "manifest", "inspect", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=dict(os.environ, DOCKER_CLI_EXPERIMENTAL="enabled"),
            )
            exists = proc.returncode == 0
        else:
            exists = response.status_code == 200

    except Exception as e:
        print(f"Error checking Docker image: {e}")
        return False

    if exists:
        with _known_docker_images_lock:
            _known_docker_images.add(name)
            with KNOWN_DOCKER_IMAGES_FILE.open("a") as f:
                print(name, file=f)

    return exists


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
        }

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

        if len(self.bins) == 0 and len(self.examples) == 0:
            raise ValueError("mzbuild config is missing pre-build target")

    @staticmethod
    def generate_cargo_build_command(
        rd: RepositoryDetails,
        bins: list[str],
        examples: list[str],
    ) -> list[str]:
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
                "CPP": "clang-cpp-18",
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
        for build in builds:
            if not rd:
                rd = build.rd
            bins.update(build.bins)
            examples.update(build.examples)
        assert rd

        ui.section(f"Common build for: {', '.join(bins | examples)}")

        cargo_build = cls.generate_cargo_build_command(rd, list(bins), list(examples))

        run_and_detect_rust_ice(cargo_build, cwd=rd.root)

        # Re-run with JSON-formatted messages and capture the output so we can
        # later analyze the build artifacts in `run`. This should be nearly
        # instantaneous since we just compiled above with the same crates and
        # features. (We don't want to do the compile above with JSON-formatted
        # messages because it wouldn't be human readable.)
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
            src_path = self.rd.cargo_target_dir() / cargo_profile / bin
            copy(src_path, bin)
        for example in self.examples:
            src_path = (
                self.rd.cargo_target_dir() / cargo_profile / Path("examples") / example
            )
            copy(src_path, Path("examples") / example)

        if self.extract:
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

        self.acquired = True

    def run(self, prep: dict[str, Any]) -> None:
        super().run(prep)
        self.build(prep)

    @cache
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

    @cache
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

    def build(self, prep: dict[type[PreImage], Any], push: bool = False) -> None:
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
            "BUILD_PROFILE": self.image.rd.profile.name,
            "ARCH_GCC": str(self.image.rd.arch),
            "ARCH_GO": self.image.rd.arch.go_str(),
            "CI_SANITIZER": str(self.image.rd.sanitizer),
        }
        f = self.write_dockerfile()

        try:
            spawn.capture(["docker", "buildx", "version"])
        except subprocess.CalledProcessError:
            if push:
                print(
                    "docker buildx not found, required to push images. Installation: https://github.com/docker/buildx?tab=readme-ov-file#installing"
                )
                raise
            print(
                "docker buildx not found, you can install it to build faster. Installation: https://github.com/docker/buildx?tab=readme-ov-file#installing"
            )
            print("Falling back to docker build")
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
        else:
            cmd: Sequence[str] = [
                "docker",
                "buildx",
                "build",
                "--progress=plain",  # less noisy
                "-f",
                "-",
                *(f"--build-arg={k}={v}" for k, v in build_args.items()),
                "-t",
                f"docker.io/{self.spec()}",
                "-t",
                f"ghcr.io/materializeinc/{self.spec()}",
                f"--platform=linux/{self.image.rd.arch.go_str()}",
                str(self.image.path),
                *(["--push"] if push else ["--load"]),
            ]

        if token := os.getenv("GITHUB_GHCR_TOKEN"):
            spawn.runv(
                [
                    "docker",
                    "login",
                    "ghcr.io",
                    "-u",
                    "materialize-bot",
                    "--password-stdin",
                ],
                stdin=token.encode(),
            )

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
                        stdin=subprocess.DEVNULL,
                        stdout=sys.stderr.buffer,
                    )
                    self.acquired = True
                    break
                except subprocess.CalledProcessError:
                    if retry < max_retries:
                        # There seems to be no good way to tell what error
                        # happened based on error code
                        # (https://github.com/docker/cli/issues/538) and we
                        # want to print output directly to terminal.
                        if build := os.getenv("CI_WAITING_FOR_BUILD"):
                            for retry in range(max_retries):
                                try:
                                    build_status = buildkite.get_build_status(build)
                                except subprocess.CalledProcessError:
                                    time.sleep(sleep_time)
                                    sleep_time = min(sleep_time * 2, 10)
                                    break
                                print(f"Build {build} status: {build_status}")
                                if build_status == "failed":
                                    print(
                                        f"Build {build} has been marked as failed, exiting hard"
                                    )
                                    sys.exit(1)
                                elif build_status == "success":
                                    break
                                assert (
                                    build_status == "pending"
                                ), f"Unknown build status {build_status}"
                                time.sleep(1)
                        else:
                            print(f"Retrying in {sleep_time}s ...")
                            time.sleep(sleep_time)
                            sleep_time = min(sleep_time * 2, 10)
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

    @cache
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

        self_hash.update(f"profile={self.image.rd.profile}".encode())
        self_hash.update(f"arch={self.image.rd.arch}".encode())
        self_hash.update(f"coverage={self.image.rd.coverage}".encode())
        self_hash.update(f"sanitizer={self.image.rd.sanitizer}".encode())
        # This exists to make sure all hashes from before we had a GHCR mirror are invalidated, so that we rebuild when an image doesn't exist on GHCR yet
        self_hash.update(b"mirror=ghcr")

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
            max_retries = (
                90
                if os.getenv("CI_WAITING_FOR_BUILD")
                else (
                    5
                    if ui.env_is_truthy("CI")
                    and not ui.env_is_truthy("CI_ALLOW_LOCAL_BUILD")
                    else 1
                )
            )
        assert max_retries > 0

        deps_to_check = [dep for dep in self if dep.publish]
        deps_to_build = [dep for dep in self if not dep.publish]
        if len(deps_to_check):
            with ThreadPoolExecutor(max_workers=len(deps_to_check)) as executor:
                futures = [
                    executor.submit(dep.try_pull, max_retries) for dep in deps_to_check
                ]
                for dep, future in zip(deps_to_check, futures):
                    try:
                        if not future.result():
                            deps_to_build.append(dep)
                    except Exception:
                        deps_to_build.append(dep)

        # Don't attempt to build in CI, as our timeouts and small machines won't allow it anyway
        if ui.env_is_truthy("CI") and not ui.env_is_truthy("CI_ALLOW_LOCAL_BUILD"):
            expected_deps = [dep for dep in deps_to_build if dep.publish]
            if expected_deps:
                print(
                    f"+++ Expected builds to be available, the build probably failed, so not proceeding: {expected_deps}"
                )
                sys.exit(5)

        prep = self._prepare_batch(deps_to_build)
        for dep in deps_to_build:
            dep.build(prep)

    def ensure(self, pre_build: Callable[[list[ResolvedImage]], None] | None = None):
        """Ensure all publishable images in this dependency set exist on Docker
        Hub.

        Images are pushed using their spec as their tag.

        Args:
            pre_build: A callback to invoke with all dependency that are going
                       to be built locally, invoked after their cargo build is
                       done, but before the Docker images are build and
                       uploaded to DockerHub.
        """
        num_deps = len(list(self))
        if not num_deps:
            deps_to_build = []
        else:
            with ThreadPoolExecutor(max_workers=num_deps) as executor:
                futures = list(
                    executor.map(
                        lambda dep: (dep, not dep.is_published_if_necessary()), self
                    )
                )

            deps_to_build = [dep for dep, should_build in futures if should_build]

        prep = self._prepare_batch(deps_to_build)
        if pre_build:
            pre_build(deps_to_build)
        lock = Lock()
        built_deps: set[str] = set([dep.name for dep in self]) - set(
            [dep.name for dep in deps_to_build]
        )

        def build_dep(dep):
            end_time = time.time() + 600
            while True:
                if time.time() > end_time:
                    raise TimeoutError(
                        f"Timed out in {dep.name} waiting for {[dep2.name for dep2 in dep.dependencies if dep2 not in built_deps]}"
                    )
                with lock:
                    if all(dep2 in built_deps for dep2 in dep.dependencies):
                        break
                time.sleep(0.01)
            for attempts_remaining in reversed(range(3)):
                try:
                    dep.build(prep, push=dep.publish)
                    with lock:
                        built_deps.add(dep.name)
                    break
                except Exception:
                    if not dep.publish or attempts_remaining == 0:
                        raise

        if deps_to_build:
            with ThreadPoolExecutor(max_workers=len(deps_to_build)) as executor:
                futures = [executor.submit(build_dep, dep) for dep in deps_to_build]
                for future in as_completed(futures):
                    future.result()

    def check(self) -> bool:
        """Check all publishable images in this dependency set exist on Docker
        Hub. Don't try to download or build them."""
        num_deps = len(list(self))
        if num_deps == 0:
            return True
        with ThreadPoolExecutor(max_workers=num_deps) as executor:
            results = list(
                executor.map(lambda dep: dep.is_published_if_necessary(), list(self))
            )
        return all(results)

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
        profile: Profile = (
            Profile.RELEASE if ui.env_is_truthy("CI_LTO") else Profile.OPTIMIZED
        ),
        coverage: bool = False,
        sanitizer: Sanitizer = Sanitizer.none,
        image_registry: str = (
            "ghcr.io/materializeinc/materialize"
            if ui.env_is_truthy("MZ_GHCR", MZ_GHCR_DEFAULT)
            else "materialize"
        ),
        image_prefix: str = "",
    ):
        self.rd = RepositoryDetails(
            root,
            arch,
            profile,
            coverage,
            sanitizer,
            image_registry,
            image_prefix,
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
            default=(
                "ghcr.io/materializeinc/materialize"
                if ui.env_is_truthy("CI")
                or ui.env_is_truthy("MZ_GHCR", MZ_GHCR_DEFAULT)
                else "materialize"
            ),
            help="the Docker image registry to pull images from and push images to",
        )
        parser.add_argument(
            "--image-prefix",
            default="",
            help="a prefix to apply to all Docker image names",
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
            profile = (
                Profile.RELEASE if ui.env_is_truthy("CI_LTO") else Profile.OPTIMIZED
            )

        return cls(
            root,
            profile=profile,
            coverage=args.coverage,
            sanitizer=args.sanitizer,
            image_registry=args.image_registry,
            image_prefix=args.image_prefix,
            arch=args.arch,
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
        if not is_docker_image_pushed(name):
            spawn.runv(
                [
                    "docker",
                    "manifest",
                    "create",
                    name,
                    *(image.spec() for image in images),
                ]
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


def tag_multiarch_images(
    new_tag: str, previous_tag: str, dependency_sets: Iterable[Iterable[ResolvedImage]]
) -> None:
    """Publishes a set of docker images under a given tag."""
    for images in zip(*dependency_sets):
        names = set(image.image.name for image in images)
        assert len(names) == 1, "dependency sets did not contain identical images"
        new_name = images[0].image.docker_name(new_tag)

        # Doesn't have tagged images
        if images[0].image.name == "mz":
            continue

        previous_name = images[0].image.docker_name(previous_tag)
        spawn.runv(["docker", "pull", previous_name])
        spawn.runv(["docker", "tag", previous_name, new_name])
        spawn.runv(["docker", "push", new_name])
    print(f"--- Nofifying for tag {new_tag}")
    markdown = f"""Pushed images with Docker tag `{new_tag}`"""
    spawn.runv(
        [
            "buildkite-agent",
            "annotate",
            "--style=info",
            f"--context=build-tags-{new_tag}",
        ],
        stdin=markdown.encode(),
    )
