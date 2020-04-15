# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from functools import lru_cache
from collections import OrderedDict
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
    REGISTRY = "registry"
    LOCAL_BUILD = "local-build"


def runv(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = None,
    stdin: Optional[IO[bytes]] = None,
) -> None:
    print("$", " ".join(str(arg) for arg in args))
    subprocess.check_call(args, cwd=cwd, stdin=stdin)


@overload
def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = ...,
    universal_newlines: Literal[False] = ...,
) -> bytes:
    ...


@overload
def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = ...,
    *,
    universal_newlines: Literal[True],
) -> str:
    ...


def capture(
    args: Sequence[Union[Path, str]],
    cwd: Optional[Path] = None,
    universal_newlines: bool = False,
) -> Union[str, bytes]:
    if universal_newlines:
        return subprocess.check_output(args, cwd=cwd, universal_newlines=True)
    else:
        return subprocess.check_output(args, cwd=cwd, universal_newlines=False)


def xcargo(root: Path) -> str:
    if sys.platform == "linux":
        return "cargo"
    else:
        return str(root / "bin" / "xcompile")


def xcargo_target_dir(root: Path) -> Path:
    if sys.platform == "linux":
        return root / "target"
    else:
        return root / "target" / "x86_64-unknown-linux-gnu"


def xbinutil(tool: str) -> str:
    if sys.platform == "linux":
        return tool
    else:
        return f"x86_64-unknown-linux-gnu-{tool}"


xobjcopy = xbinutil("objcopy")
xstrip = xbinutil("strip")


def docker_images() -> Set[str]:
    return set(
        capture(
            ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
            universal_newlines=True,
        )
        .strip()
        .split("\n")
    )


def git_ls_files(root: Path, *specs: Union[Path, str]) -> List[bytes]:
    files = capture(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", *specs],
        cwd=root,
    ).split(b"\0")
    return [f for f in files if f.strip() != b""]


def chmod_x(path: Path) -> None:
    # https://stackoverflow.com/a/30463972/1122351
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2  # copy R bits to X
    os.chmod(path, mode)


class PreImage:
    def run(self, root: Path, path: Path) -> None:
        runv(["git", "clean", "-ffdX", path])

    def depends(self, root: Path, path: Path) -> List[bytes]:
        # HACK(benesch): all Rust code implicitly depends on the ci-builder
        # version. Can we express this with less hardcoding?
        return [b"ci/builder/stable.stamp", b"ci/builder/nightly.stamp"]


class CargoBuild(PreImage):
    def __init__(self, config: Dict[str, Any]):
        self.bin = config.pop("bin", None)
        self.strip = config.pop("strip", True)
        if self.bin is None:
            raise ValueError("mzbuild config is missing pre-build target")

    def run(self, root: Path, path: Path) -> None:
        super().run(root, path)
        runv([xcargo(root), "build", "--release", "--bin", self.bin], cwd=root)
        shutil.copy(xcargo_target_dir(root) / "release" / self.bin, path)
        if self.strip:
            # NOTE(benesch): the debug information is large enough that it slows
            # down CI, since we're packaging these binaries up into Docker
            # images and shipping them around. A bit unfortunate, since it'd be
            # nice to have useful backtraces if the binary crashes.
            runv([xstrip, path / self.bin])
        else:
            # Even if we've been asked not to strip the binary, remove the
            # `.debug_pubnames` and `.debug_pubtypes` sections. These are just
            # indexes that speed up launching a debugger against the binary,
            # and we're happy to have slower debugger start up in exchange for
            # smaller binaries. Plus the sections have been obsoleted by a
            # `.debug_names` section in DWARF 5, and so debugger support for
            # `.debug_pubnames`/`.debug_pubtypes` is minimal anyway.
            # See: https://github.com/rust-lang/rust/issues/46034
            runv(
                [
                    xobjcopy,
                    "-R",
                    ".debug_pubnames",
                    "-R",
                    ".debug_pubtypes",
                    path / self.bin,
                ]
            )

    def depends(self, root: Path, path: Path) -> List[bytes]:
        # TODO(benesch): this should be much smarter about computing the Rust
        # files that actually contribute to this binary target.
        return super().depends(root, path) + git_ls_files(
            root, "demo/billing/**", "src/**", "Cargo.toml", "Cargo.lock", ".cargo"
        )


# TODO(benesch): make this less hardcoded and custom.
class CargoTest(PreImage):
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
        runv(args)

        tests = []
        for line in capture(
            args + ["--message-format=json"], universal_newlines=True
        ).split("\n"):
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
                runv([xstrip, path / "tests" / slug])
                manifest.write(f"{slug} {crate_path}\n")
        shutil.move(str(path / "testdrive"), path / "tests")
        shutil.copy(
            xcargo_target_dir(root) / "debug" / "examples" / "pingpong",
            path / "tests" / "examples",
        )
        shutil.copytree(root / "misc" / "shlib", path / "shlib")

    def depends(self, root: Path, path: Path) -> List[bytes]:
        # TODO(benesch): this should be much smarter about computing the Rust
        # files that actually contribute to this binary target.
        return super().depends(root, path) + git_ls_files(
            root, "demo/billing/**", "src/**", "Cargo.toml", "Cargo.lock", ".cargo"
        )


class Image:
    DOCKERFILE_MZFROM_RE = re.compile(rb"^MZFROM\s*(\S+)")

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
                match = self.DOCKERFILE_MZFROM_RE.match(line)
                if match:
                    self.depends_on.append(match.group(1).decode())

    def write_dockerfile(self, dep_specs: Dict[str, str]) -> IO[bytes]:
        with open(self.path / "Dockerfile", "rb") as f:
            lines = f.readlines()
        f = TemporaryFile()
        for line in lines:
            match = self.DOCKERFILE_MZFROM_RE.match(line)
            if match:
                image = match.group(1).decode()
                spec = dep_specs[image]
                line = self.DOCKERFILE_MZFROM_RE.sub(b"FROM %b" % spec.encode(), line)
            f.write(line)
        f.seek(0)
        return f

    def env_var_name(self) -> str:
        return self.name.upper().replace("-", "_")

    def docker_name(self, tag: str) -> str:
        return f"materialize/{self.name}:{tag}"

    def spec(self) -> str:
        return self.docker_name(f"mzbuild-{self.fingerprint()}")

    def build(self, dep_specs: Dict[str, str]) -> None:
        if self.pre_image is not None:
            self.pre_image.run(self.root, self.path)
        f = self.write_dockerfile(dep_specs)
        runv(
            ["docker", "build", "--pull", "-f", "-", "-t", self.spec(), self.path],
            stdin=f,
        )

    def acquire(self, dep_specs: Dict[str, str]) -> AcquiredFrom:
        if self.publish:
            try:
                runv(["docker", "pull", self.spec()])
                return AcquiredFrom.REGISTRY
            except subprocess.CalledProcessError:
                pass
        self.build(dep_specs)
        return AcquiredFrom.LOCAL_BUILD

    def push(self) -> None:
        runv(["docker", "push", self.spec()])

    def run(self, args: List[str] = []) -> None:
        runv(["docker", "run", "-it", "--rm", "--init", self.spec(), *args])

    @lru_cache(maxsize=None)
    def fingerprint(self) -> str:
        paths = git_ls_files(self.root, self.path)
        if self.pre_image is not None:
            paths += self.pre_image.depends(self.root, self.path)
        paths.sort()
        fingerprint = hashlib.sha1()
        for rel_path in paths:
            abs_path = self.root / rel_path.decode()
            file_hash = hashlib.sha1()
            file_mode = stat.S_IMODE(os.lstat(abs_path).st_mode)
            with open(abs_path, "rb") as f:
                file_hash.update(f.read())
            fingerprint.update(file_mode.to_bytes(2, byteorder="big"))
            fingerprint.update(rel_path)
            fingerprint.update(file_hash.digest())
            fingerprint.update(b"\0")
        return base64.b32encode(fingerprint.digest()).decode()


class DependencySet:
    def __init__(self, dependencies: Iterable[Image]):
        self.dependencies = list(dependencies)

    def acquire(self, force_build: bool = False, push: bool = False) -> None:
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
