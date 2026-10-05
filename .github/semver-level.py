#! /usr/bin/env python3
import re
import sys


SUBJECT = re.compile(r"^(?P<type>[a-z]+)(?:\([^\r\n]*\))?(?P<breaking>!)?:")
BREAKING_CHANGE = re.compile(r"^BREAKING(?: CHANGE|-CHANGE):", re.MULTILINE)


def semver_level(commit_log):
    """Read git log --pretty=format:'%s%x1f%b%x1e' and return the highest bump."""
    level = None
    for record in commit_log.split("\x1e"):
        record = record.lstrip("\r\n")
        if not record:
            continue
        subject, body = record.split("\x1f", 1)
        match = SUBJECT.match(subject)
        if (
            (match and match.group("breaking"))
            or BREAKING_CHANGE.search(body)
            or BREAKING_CHANGE.match(subject)
        ):
            return "major"
        if match:
            if match.group("type") in {"feat", "revert"}:
                level = "minor"
            elif level is None:
                level = "patch"
    return level


if __name__ == "__main__":
    level = semver_level(sys.stdin.read())
    if level is None:
        sys.exit(1)
    print(level)
