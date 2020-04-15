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
from typing import cast, Any, Dict, List, Set, Optional, Union, Iterable, Iterator, IO
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


def runv(args: List[Union[Path, str]],
         cwd: Optional[Path] = None,
         stdin: Optional[IO[bytes]] = None) -> None:
    args = [str(arg) for arg in args]  # type: List[str]
    cwd = str(cwd) if cwd is not None else None  # type: Optional[str]
    print("$", " ".join(args))
    subprocess.check_call(args, cwd=cwd, stdin=stdin)


def capture(args: List[Union[Path, str]],
            cwd: Optional[Path] = None,
            universal_newlines: bool = False) -> Any:
    args = [str(arg) for arg in args]  # type: List[str]
    cwd = str(cwd) if cwd is not None else None  # type: Optional[str]
    return subprocess.check_output(args,
                                   cwd=cwd,
                                   universal_newlines=universal_newlines)


def xcargo(root: Path) -> Path:
    if sys.platform == "linux":
        return "cargo"
    else:
        return root / "bin" / "xcompile"


def xcargo_target_dir(root: Path) -> Path:
    if sys.platform == "linux":
        return root / "target"
    else:
        return root / "target" / "x86_64-unknown-linux-gnu"


def xstrip(root: Path) -> str:
    if sys.platform == "linux":
        return "strip"
    else:
        return "x86_64-unknown-linux-gnu-strip"


def docker_images() -> Set[str]:
    return set(
        capture(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"],
                universal_newlines=True).strip().split("\n"))


def git_ls_files(root: Path, *specs: str) -> List[bytes]:
    files = capture([
        "git", "ls-files", "--cached", "--others", "--exclude-standard", "-z",
        *specs
    ],
                    cwd=root).split(b"\0")
    return [f for f in files if f.strip() != b""]


def chmod_x(path: Path) -> None:
    # https://stackoverflow.com/a/30463972/1122351
    mode = os.stat(str(path)).st_mode
    mode |= (mode & 0o444) >> 2  # copy R bits to X
    os.chmod(str(path), mode)


class PreImage:
    def run(self, root: Path, path: Path) -> None:
        runv(["git", "clean", "-ffX", path])

    def depends(self, root: Path, path: Path) -> List[bytes]:
        pass


class CargoBuild(PreImage):
    def __init__(self, config: Dict[str, Any]):
        self.bin = config.pop("bin", None)
        self.strip = config.pop("strip", True)
        if self.bin is None:
            raise ValueError("mzbuild config is missing pre-build target")

    def run(self, root: Path, path: Path) -> None:
        super().run(root, path)
        runv([xcargo(root), "build", "--release", "--bin", self.bin], cwd=root)
        shutil.copy(str(xcargo_target_dir(root) / "release" / self.bin),
                    str(path))
        if self.strip:
            # NOTE(benesch): the debug information is large enough that it slows
            # down CI, since we're packaging these binaries up into Docker
            # images and shipping them around. A bit unfortunate, since it'd be
            # nice to have useful backtraces if the binary crashes.
            runv([xstrip(root), path / self.bin])

    def depends(self, root: Path, path: Path) -> List[bytes]:
        # TODO(benesch): this should be much smarter about computing the Rust
        # files that actually contribute to this binary target.
        return git_ls_files(root, "src/**", "Cargo.toml", "Cargo.lock")


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
        args = [xcargo(root), "test", "--locked",
                "--no-run"]  # type: List[Union[str,Path]]
        runv(args)

        tests = []
        for message in capture(args + ["--message-format=json"],
                               universal_newlines=True).split("\n"):
            if message.strip() == "":
                continue
            message = json.loads(message)
            if message.get("profile", {}).get("test", False):
                crate_name = message["package_id"].split()[0]
                target_kind = "".join(message["target"]["kind"])
                slug = crate_name + "." + target_kind
                if target_kind != "lib":
                    slug += "." + message["target"]["name"]
                crate_path_match = re.search("\(path\+file://(.*)\)",
                                             message["package_id"])
                if not crate_path_match:
                    raise ValueError("invalid package_id: {}".format(
                        message["package_id"]))
                crate_path = Path(crate_path_match.group(1)).relative_to(
                    root.resolve())
                tests.append((message["executable"], slug, crate_path))

        os.makedirs(str(path / "tests" / "examples"))
        with open(str(path / "tests" / "manifest"), "w") as manifest:
            for (executable, slug, crate_path) in tests:
                shutil.copy(str(executable), str(path / "tests" / slug))
                runv([xstrip(root), path / "tests" / slug])
                manifest.write("{} {}\n".format(slug, crate_path))
        shutil.move(str(path / "testdrive"), str(path / "tests"))
        shutil.copy(
            str(xcargo_target_dir(root) / "debug" / "examples" / "pingpong"),
            str(path / "tests" / "examples"))
        shutil.copytree(str(root / "misc" / "shlib"), str(path / "shlib"))

    def depends(self, root: Path, path: Path) -> List[bytes]:
        # TODO(benesch): this should be much smarter about computing the Rust
        # files that actually contribute to this binary target.
        return git_ls_files(root, "src/**", "Cargo.toml", "Cargo.lock")


class Image:
    DOCKERFILE_MZFROM_RE = re.compile(rb"^MZFROM\s*(\S+)")

    def __init__(self, root: Path, path: Path):
        self.root = root
        self.path = path
        self.pre_image = None  # type: Optional[PreImage]
        with open(str(self.path / "mzbuild.yml")) as f:
            data = yaml.safe_load(f)
            self.name = data.pop("name", None)
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
                        "mzbuild config in {} has unknown pre-image type".
                        format(self.path))

        if re.search(r"[^A-Za-z0-9\-]", self.name):
            raise ValueError(
                "mzbuild image name {} contains invalid character; only alphanumerics and hyphens allowed"
                .format(self.name))

        self.depends_on = []
        with open(str(self.path / "Dockerfile"), "rb") as f:
            for line in f:
                match = self.DOCKERFILE_MZFROM_RE.match(line)
                if match:
                    self.depends_on.append(match.group(1).decode())

    def write_dockerfile(self, dep_specs: Dict[str, str]) -> IO[bytes]:
        with open(str(self.path / "Dockerfile"), "rb") as f:
            lines = f.readlines()
        f = TemporaryFile()
        for line in lines:
            match = self.DOCKERFILE_MZFROM_RE.match(line)
            if match:
                image = match.group(1).decode()
                spec = dep_specs[image]
                line = self.DOCKERFILE_MZFROM_RE.sub(
                    b"FROM %b" % spec.encode(), line)
            f.write(line)
        f.seek(0)
        return f

    def env_var_name(self) -> str:
        return self.name.upper().replace("-", "_")

    def docker_name(self, tag: str) -> str:
        return "materialize/{}:{}".format(self.name, tag)

    def spec(self) -> str:
        return self.docker_name("mzbuild-{}".format(self.fingerprint()))

    def build(self, dep_specs: Dict[str, str]) -> None:
        if self.pre_image is not None:
            self.pre_image.run(self.root, self.path)
        f = self.write_dockerfile(dep_specs)
        runv([
            "docker", "build", "--pull", "-f", "-", "-t",
            self.spec(), self.path
        ],
             stdin=f)

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
        paths = git_ls_files(self.root, str(self.path))
        if self.pre_image is not None:
            paths += self.pre_image.depends(self.root, self.path)
        paths.sort()
        fingerprint = hashlib.sha1()
        for rel_path in paths:
            abs_path = str(self.root / rel_path.decode())
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
                    print("==> Force-building {}".format(spec))
                    d.build(dep_specs)
                else:
                    print("==> Acquiring {}".format(spec))
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
        self.images = {}  # type: Dict[str, Image]
        self.compose_dirs = set()
        for (path, dirs, files) in os.walk(str(self.root), topdown=True):
            # Filter out some particularly massive ignored directories to keep
            # things snappy. Not required for correctness.
            dirs[:] = set(dirs) - {".git", "target", "mzdata", "node_modules"}
            if "mzbuild.yml" in files:
                image = Image(self.root, Path(path))
                if not image.name:
                    raise ValueError("config at {} missing name".format(path))
                if image.name in self.images:
                    raise ValueError("image {} exists twice".format(
                        image.name))
                self.images[image.name] = image
            if "mzcompose.yml" in files:
                self.compose_dirs.add(Path(path))

        # Validate dependencies.
        for image in self.images.values():
            for d in image.depends_on:
                if d not in self.images:
                    raise ValueError(
                        "image {} depends on non-existent image {}".format(
                            image.name, d))

    def resolve_dependencies(self, targets: Iterable[Image]) -> DependencySet:
        resolved = OrderedDict()
        visiting = set()

        def visit(image: Image, path: List[str] = []) -> None:
            if image.name in resolved:
                return
            if image.name in visiting:
                raise ValueError("circular dependency in mzbuild: {}".format(
                    " -> ".join(path + [image.name])))

            visiting.add(image.name)
            for d in sorted(image.depends_on):
                visit(self.images[d], path + [image.name])
            resolved[image.name] = image

        for target in sorted(targets, key=lambda image: image.name):
            visit(target)

        return DependencySet(resolved.values())

    def __iter__(self) -> Iterator[Image]:
        return iter(self.images.values())
