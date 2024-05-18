load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

"""Download all of the repositories necessary to setup the clang toolchain."""

def _download_clang_release(name, version, platform, checksum):
    """
    Downloads a release of LLVM/clang based on its platform and version.
    """

    URL_BASE = "https://github.com/llvm/llvm-project/releases/download/"

    suffix = "clang+llvm-{version}-{platform}".format(
        version = version,
        platform = platform,
    )
    url = "{base}llvmorg-{version}/{suffix}.tar.gz".format(
        base = URL_BASE,
        version = version,
        suffix = suffix,
    )

    maybe(
        http_archive,
        name = name,
        sha256 = checksum,
        strip_prefix = suffix,
        urls = [url],
        build_file = "//misc/bazel/third_party/toolchains/clang:BUILD.clang.bazel",
    )
