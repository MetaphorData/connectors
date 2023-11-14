#!/usr/bin/env bash

if git diff --name-only origin/main | grep ".*\.py$" > /dev/null 2>&1; then
    # Found modified stuff, make sure version is bumped!
    MAIN_TAG=$(git describe origin/main --tags --abbrev=0)
    CURRENT_VERSION=$(poetry version --short 2>/dev/null)

    GIT_ROOT=$(git rev-parse --show-toplevel)
    # Script exits with return code 1 if the check fails.
    python ${GIT_ROOT}/.github/scripts/compare_versions.py ${MAIN_TAG} ${CURRENT_VERSION}
fi