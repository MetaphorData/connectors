#!/usr/bin/env bash

if git diff --name-only origin/main | grep ".*\.py$" > /dev/null 2>&1; then
    # Found modified stuff, make sure version is bumped!
    MAIN_TAG=$(git describe origin/main --tags --abbrev=0)
    CURRENT_VERSION=$(poetry version --short 2>/dev/null)

    # Check if current version is larger than latest main version.
    # Note: We want this program to exit 1 when the check fails. In Python `True` == `1`, so we want this
    # comparison to evaluate to `0` if we want the check to pass.
    LINES=(
        "from packaging import version" 
        "import sys" 
        "sys.exit(version.parse(\"$MAIN_TAG\") >= version.parse(\"$CURRENT_VERSION\"))"
    )
    # Concat the lines and pass them to python
    printf '%s;' "${LINES[@]}" | python
fi