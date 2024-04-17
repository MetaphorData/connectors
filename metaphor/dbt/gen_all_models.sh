#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
cd ${SCRIPT_DIR}

MANIFEST_VERSIONS=" \
  v5 \
  v6 \
  v7 \
  v8 \
  v9 \
  v10 \
  v11 \
  v12 \
"

for version in ${MANIFEST_VERSIONS}; do
  ./gen_models.sh manifest ${version}
done

RUN_RESULTS_VERSIONS="\
  v4 \
  v5 \
  v6 \
"

for version in ${RUN_RESULTS_VERSIONS}; do
  ./gen_models.sh run-results ${version}
done
