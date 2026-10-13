#! /usr/bin/env python3
import re
import sys


VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", re.ASCII)
TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", re.ASCII)


def validate_version(version, sorted_tags):
    """Return version if it is X.Y.Z, above the largest tag, and not yet tagged.

    sorted_tags is the output of `git tag --list 'v*' | sort -V`, split into lines.
    Raises ValueError otherwise.
    """
    if not VERSION.fullmatch(version):
        raise ValueError(f"version must be X.Y.Z (digits only): {version!r}")
    tags = [tag for tag in sorted_tags if tag]
    if not tags:
        raise ValueError("no existing v* tag found")
    if f"v{version}" in tags:
        raise ValueError(f"tag v{version} already exists")
    latest = TAG.fullmatch(tags[-1])
    if not latest:
        raise ValueError(f"latest tag is not vX.Y.Z: {tags[-1]!r}")
    if tuple(map(int, version.split("."))) <= tuple(map(int, latest.groups())):
        raise ValueError(f"version {version} must be greater than the latest tag {tags[-1]}")
    return version


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: release-version.py <version>", file=sys.stderr)
        sys.exit(1)
    try:
        print(validate_version(sys.argv[1], sys.stdin.read().splitlines()))
    except ValueError as error:
        print(error, file=sys.stderr)
        sys.exit(1)
