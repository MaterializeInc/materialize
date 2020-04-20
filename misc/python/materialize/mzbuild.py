# Copyright Materialize, Inc. All rights reserved.
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

[user-docs]: https://github.com/MaterializeInc/materialize/blob/master/doc/developer/mzbuild.md
"""

from collections import OrderedDict
from functools import lru_cache
from materialize import spawn
from pathlib import Path
from tempfile import TemporaryFile
from typing import (
    cast,
    Any,
    Dict,
    List,
    Set,
    Sequence,
    Optional,
    Union,
    Iterable,
    Iterator,
    IO,
    overload,
)
from typing_extensions import Literal
import base64
import contextlib
import enum
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import yaml


class AcquiredFrom(enum.Enum):
    """Where an `Image` was acquired from."""

    REGISTRY = "registry"
    """The image was downloaded from Docker Hub."""

    LOCAL_BUILD = "local-build"
    """The image was built from source locally."""


def xcargo(root: Path) -> str:
    """Determine the path to a `cargo` executable that targets linux/amd64."""
    if sys.platform == "linux":
        return "cargo"
    else:
        return str(root / "bin" / "xcompile")


def xcargo_target_dir(root: Path) -> Path:
    """Determine the path to the target directory for Cargo. """
    if sys.platform == "linux":
        return root / "target"
    else:
        return root / "target" / "x86_64-unknown-linux-gnu"


def xbinutil(tool: str) -> str:
    """Determine the name of a binutils tool for linux/amd64."""
    if sys.platform == "linux":
        return tool
    else:
        return f"x86_64-unknown-linux-gnu-{tool}"


xobjcopy = xbinutil("objcopy")
xstrip = xbinutil("strip")


def docker_images() -> Set[str]:
    """List the Docker images available on the local machine."""
    return set(
        spawn.capture(
            ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], unicode=True,
        )
        .strip()
        .split("\n")
    )


def git_ls_files(root: Path, *specs: Union[Path, str]) -> List[bytes]:
    """Find unignored files within the specified paths."""
    files = spawn.capture(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", *specs],
        cwd=root,
    ).split(b"\0")
    return [f for f in files if f.strip() != b""]


def chmod_x(path: Path) -> None:
    """Set the executable bit on a file or directory."""
    # https://stackoverflow.com/a/30463972/1122351
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2  # copy R bits to X
    os.chmod(path, mode)


class PreImage:
    """An action to run before building a Docker image."""

    def run(self, root: Path, path: Path) -> None:
        """Perform the action.

        Args:
            root: The path to the root of the `Repository` associated with this
                action.
            path: The path to the `Image` associated with this action.
        """
        spawn.runv(["git", "clean", "-ffdX", path])

    def inputs(self, root: Path, path: Path) -> List[bytes]:
        """Return the paths which are considered inputs to the action.

        Args:
            root: The path to the root of the `Repository` associated with this
                action.
            path: The path to the `Image` associated with this action.
        """
        # HACK(benesch): all Rust code implicitly depends on the ci-builder
        # version. Can we express this with less hardcoding?
        return [b"ci/builder/stable.stamp", b"ci/builder/nightly.stamp"]


class CargoBuild(PreImage):
    """A pre-image action that builds a single binary with Cargo."""

    def __init__(self, config: Dict[str, Any]):
        self.bin = config.pop("bin", None)
        self.strip = config.pop("strip", True)
        if self.bin is None:
            raise ValueError("mzbuild config is missing pre-build target")

    def run(self, root: Path, path: Path) -> None:
        super().run(root, path)
        spawn.runv([xcargo(root), "build", "--release", "--bin", self.bin], cwd=root)
        shutil.copy(xcargo_target_dir(root) / "release" / self.bin, path)
        if self.strip:
            # NOTE(benesch): the debug information is large enough that it slows
            # down CI, since we're packaging these binaries up into Docker
            # images and shipping them around. A bit unfortunate, since it'd be
            # nice to have useful backtraces if the binary crashes.
            spawn.runv([xstrip, path / self.bin])
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
                    xobjcopy,
                    "-R",
                    ".debug_pubnames",
                    "-R",
                    ".debug_pubtypes",
                    path / self.bin,
                ]
            )

    def inputs(self, root: Path, path: Path) -> List[bytes]:
        # TODO(benesch): this should be much smarter about computing the Rust
        # files that actually contribute to this binary target.
        return super().inputs(root, path) + git_ls_files(
            root, "demo/billing/**", "src/**", "Cargo.toml", "Cargo.lock", ".cargo"
        )


# TODO(benesch): make this less hardcoded and custom.
class CargoTest(PreImage):
    """A pre-image action that builds all test binaries in the Cargo workspace.

    .. todo:: This action does not generalize well.
        Its implementation currently hardcodes many details about the
        dependencies of various Rust crates. Ideally these dependencies would
        instead be captured by configuration in `mzbuild.yml`.
    """

    def __init__(self, config: Dict[str, Any]):
        pass

    def run(self, root: Path, path: Path) -> None:
        super().run(root, path)
        CargoBuild({"bin": "testdrive"}).run(root, path)

        # NOTE(benesch): The two invocations of `cargo test --no-run` here
        # deserve some explanation. The first invocation prints error messages
        # to stdout in a human readable form. If that succeeds, the second
        # invocation instructs Cargo to dump the locations of the test binaries
        # it built in a machine readable form. Without the first invocation, the
        # error messages would also be sent to the output file in JSON, and the
        # user would only see a vague "could not compile <package>" error.
        args = [
            xcargo(root),
            "test",
            "--locked",
            "--no-run",
        ]
        spawn.runv(args)
        output = spawn.capture(args + ["--message-format=json"], unicode=True)

        tests = []
        for line in output.split("\n"):
            if line.strip() == "":
                continue
            message = json.loads(line)
            if message.get("profile", {}).get("test", False):
                crate_name = message["package_id"].split()[0]
                target_kind = "".join(message["target"]["kind"])
                slug = crate_name + "." + target_kind
                if target_kind != "lib":
                    slug += "." + message["target"]["name"]
                crate_path_match = re.search(
                    r"\(path\+file://(.*)\)", message["package_id"]
                )
                if not crate_path_match:
                    raise ValueError(f'invalid package_id: {message["package_id"]}')
                crate_path = Path(crate_path_match.group(1)).relative_to(root.resolve())
                tests.append((message["executable"], slug, crate_path))

        os.makedirs(path / "tests" / "examples")
        with open(path / "tests" / "manifest", "w") as manifest:
            for (executable, slug, crate_path) in tests:
                shutil.copy(executable, path / "tests" / slug)
                spawn.runv([xstrip, path / "tests" / slug])
                manifest.write(f"{slug} {crate_path}\n")
        shutil.move(str(path / "testdrive"), path / "tests")
        shutil.copy(
            xcargo_target_dir(root) / "debug" / "examples" / "pingpong",
            path / "tests" / "examples",
        )
        shutil.copytree(root / "misc" / "shlib", path / "shlib")

    def inputs(self, root: Path, path: Path) -> List[bytes]:
        # TODO(benesch): this should be much smarter about computing the Rust
        # files that actually contribute to this binary target.
        return super().inputs(root, path) + git_ls_files(
            root, "demo/billing/**", "src/**", "Cargo.toml", "Cargo.lock", ".cargo"
        )


class Image:
    """An Docker image whose build and dependencies are managed by mzbuild.

    An image corresponds to a directory in a repository that contains a
    `mzbuild.yml` file. This directory is called an "mzbuild context."

    Attributes:
        name: The name of the image.
        publish: Whether the image should be pushed to Docker Hub.
        depends_on: The names of the images upon which this image depends.
        root: The path to the root of the associated `Repository`.
        path: The path to the directory containing the `mzbuild.yml`
            configuration file.
        pre_image: An optional action to perform before running `docker build`.
    """

    _DOCKERFILE_MZFROM_RE = re.compile(rb"^MZFROM\s*(\S+)")

    def __init__(self, root: Path, path: Path):
        self.root = root
        self.path = path
        self.pre_image: Optional[PreImage] = None
        with open(self.path / "mzbuild.yml") as f:
            data = yaml.safe_load(f)
            self.name: str = data.pop("name")
            self.publish = data.pop("publish", True)
            pre_image = data.pop("pre-image", None)
            if pre_image is not None:
                typ = pre_image.pop("type", None)
                if typ == "cargo-build":
                    self.pre_image = CargoBuild(pre_image)
                elif typ == "cargo-test":
                    self.pre_image = CargoTest(pre_image)
                else:
                    raise ValueError(
                        f"mzbuild config in {self.path} has unknown pre-image type"
                    )

        if re.search(r"[^A-Za-z0-9\-]", self.name):
            raise ValueError(
                f"mzbuild image name {self.name} contains invalid character; only alphanumerics and hyphens allowed"
            )

        self.depends_on = []
        with open(self.path / "Dockerfile", "rb") as f:
            for line in f:
                match = self._DOCKERFILE_MZFROM_RE.match(line)
                if match:
                    self.depends_on.append(match.group(1).decode())

    def write_dockerfile(self, dep_specs: Dict[str, str]) -> IO[bytes]:
        """Render the Dockerfile without mzbuild directives.

        The arguments are the same as for `Image.acquire`.

        Returns:
            file: A handle to a temporary file containing the adjusted
                Dockerfile."""
        with open(self.path / "Dockerfile", "rb") as f:
            lines = f.readlines()
        f = TemporaryFile()
        for line in lines:
            match = self._DOCKERFILE_MZFROM_RE.match(line)
            if match:
                image = match.group(1).decode()
                spec = dep_specs[image]
                line = self._DOCKERFILE_MZFROM_RE.sub(b"FROM %b" % spec.encode(), line)
            f.write(line)
        f.seek(0)
        return f

    def env_var_name(self) -> str:
        """Return the image name formatted for use in an environment variable.

        The name is capitalized and all hyphens are replaced with underscores.
        """
        return self.name.upper().replace("-", "_")

    def docker_name(self, tag: str) -> str:
        """Return the name of the image on Docker Hub at the given tag."""
        return f"materialize/{self.name}:{tag}"

    def spec(self) -> str:
        """Return the "spec" for the image.

        A spec is the unique identifier for the image given its current
        fingerprint. It is a valid Docker Hub name.
        """
        return self.docker_name(f"mzbuild-{self.fingerprint()}")

    def build(self, dep_specs: Dict[str, str]) -> None:
        """Build the image from source.

        The arguments are the same as for `Image.acquire`.
        """
        if self.pre_image is not None:
            self.pre_image.run(self.root, self.path)
        f = self.write_dockerfile(dep_specs)
        spawn.runv(
            ["docker", "build", "--pull", "-f", "-", "-t", self.spec(), self.path],
            stdin=f,
        )

    def acquire(self, dep_specs: Dict[str, str]) -> AcquiredFrom:
        """Download or build the image.

        Args:
            dep_specs: A mapping from spec to fingerprint for all images upon
                which this image depends.

        Returns:
            acquired_from: How the image was acquired.
        """
        if self.publish:
            try:
                spawn.runv(["docker", "pull", self.spec()])
                return AcquiredFrom.REGISTRY
            except subprocess.CalledProcessError:
                pass
        self.build(dep_specs)
        return AcquiredFrom.LOCAL_BUILD

    def push(self) -> None:
        """Push the image to Docker Hub.

        The image is pushed under the Docker identifier returned by
        `Image.spec`.
        """
        spawn.runv(["docker", "push", self.spec()])

    def run(self, args: List[str] = []) -> None:
        """Run a command in the image.

        Creates a container from the image and runs the command described by
        `args` in the image.
        """
        spawn.runv(["docker", "run", "-it", "--rm", "--init", self.spec(), *args])

    @lru_cache(maxsize=None)
    def fingerprint(self) -> str:
        """Fingerprints the inputs to the image.

        Returns a base32-encoded SHA-1 hash of all of the files present in the
        image. Changing the contents of any of the files or adding or removing
        files to the image will change the fingerprint.

        The image considers all non-gitignored files in its mzbuild context to
        be inputs. If it has a pre-image action, that action may add additional
        inputs via `PreImage.inputs`.
        """
        paths = git_ls_files(self.root, self.path)
        if self.pre_image is not None:
            paths += self.pre_image.inputs(self.root, self.path)
        paths.sort()
        fingerprint = hashlib.sha1()
        for rel_path in paths:
            abs_path = self.root / rel_path.decode()
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
            fingerprint.update(file_mode.to_bytes(2, byteorder="big"))
            fingerprint.update(rel_path)
            fingerprint.update(file_hash.digest())
            fingerprint.update(b"\0")
        return base64.b32encode(fingerprint.digest()).decode()


class DependencySet:
    """A topologically-sorted, transitively-closed list of `Image`s.

    .. warning:: These guarantees are not enforced.
        Dependency sets constructed by `Repository.resolve_dependencies` will
        uphold the topological sort and transitive closure guarantees, but
        it is your responsibility to provide these guarantees for any dependency
        sets you create yourself.

    Iterating over a dependency set yields the contained images in the stored
    order, with the caveat noted above. Indexing a dependency set yields the
    _i_ th item in the stored order.
    """

    def __init__(self, dependencies: Iterable[Image]):
        self.dependencies = list(dependencies)

    def acquire(self, force_build: bool = False, push: bool = False) -> None:
        """Download or build all of the images in the dependency set.

        Args:
            force_build: Whether to force all images that are not already
                available locally to be built from source, regardless of whether
                the image is available for download. Note that this argument has
                no effect if the image is already available locally.
            push: Whether to push any images that will built locally to Docker
                Hub.
        """
        known_images = docker_images()
        specs = {d.name: d.spec() for d in self}
        for d in self:
            spec = d.spec()
            dep_specs = {d: specs[d] for d in d.depends_on}
            if spec not in known_images:
                if force_build:
                    print(f"==> Force-building {spec}")
                    d.build(dep_specs)
                else:
                    print(f"==> Acquiring {spec}")
                    acquired_from = d.acquire(dep_specs)
                    if push and d.publish and acquired_from == AcquiredFrom.LOCAL_BUILD:
                        d.push()

    def __iter__(self) -> Iterator[Image]:
        return iter(self.dependencies)

    def __getitem__(self, key: int) -> Image:
        return self.dependencies[key]


class Repository:
    """A collection of mzbuild `Image`s.

    Creating a repository will walk the filesystem beneath `root` to
    automatically discover all contained `Image`s.

    Iterating over a repository yields the contained images in an arbitrary
    order.

    Args:
        root: The path to the root of the repository.

    Attributes:
        images: A mapping from image name to `Image` for all contained images.
        compose_dirs: The set of directories containing an `mzcompose.yml` file.
    """

    def __init__(self, root: Path):
        self.root = root
        self.images: Dict[str, Image] = {}
        self.compose_dirs = set()
        for (path, dirs, files) in os.walk(self.root, topdown=True):
            # Filter out some particularly massive ignored directories to keep
            # things snappy. Not required for correctness.
            dirs[:] = set(dirs) - {".git", "target", "mzdata", "node_modules"}
            if "mzbuild.yml" in files:
                image = Image(self.root, Path(path))
                if not image.name:
                    raise ValueError(f"config at {path} missing name")
                if image.name in self.images:
                    raise ValueError(f"image {image.name} exists twice")
                self.images[image.name] = image
            if "mzcompose.yml" in files:
                self.compose_dirs.add(Path(path))

        # Validate dependencies.
        for image in self.images.values():
            for d in image.depends_on:
                if d not in self.images:
                    raise ValueError(
                        f"image {image.name} depends on non-existent image {d}"
                    )

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

        def visit(image: Image, path: List[str] = []) -> None:
            if image.name in resolved:
                return
            if image.name in visiting:
                diagram = " -> ".join(path + [image.name])
                raise ValueError(f"circular dependency in mzbuild: {diagram}")

            visiting.add(image.name)
            for d in sorted(image.depends_on):
                visit(self.images[d], path + [image.name])
            resolved[image.name] = image

        for target in sorted(targets, key=lambda image: image.name):
            visit(target)

        return DependencySet(resolved.values())

    def __iter__(self) -> Iterator[Image]:
        return iter(self.images.values())
