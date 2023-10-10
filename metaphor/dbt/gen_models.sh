#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
cd ${SCRIPT_DIR}

# Generate various data models for dbt manifest & catalog using official JSON schemas

if [ $# -ne 2 ]; then
  echo "Usage: $0 <manifest or catalog> <version: v1, v2...>"
  exit 1
fi

SCHEMA="$1"
VERSION="$2"

URL=https://schemas.getdbt.com/dbt/${SCHEMA}/${VERSION}.json

OUTPUT=generated/dbt_${SCHEMA}_${VERSION}.py

CLASS_NAME=""
if [[ "${SCHEMA}" == "manifest" ]]; then
  CLASS_NAME="DbtManifest"
elif [[ "${SCHEMA}" == "catalog" ]]; then
  CLASS_NAME="DbtCatalog"
else
  echo -e "Choose either 'manifest' or 'catalog'"
  exit  1
fi

poetry run datamodel-codegen \
  --url ${URL} \
  --class-name ${CLASS_NAME} \
  --enum-field-as-literal all \
  --input-file-type jsonschema \
  --output ${OUTPUT}

# Disable mypy type-checking for generated files
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' ${OUTPUT}
