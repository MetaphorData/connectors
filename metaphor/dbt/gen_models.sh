#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
cd ${SCRIPT_DIR}

# Generate various data models for dbt manifest & catalog using official JSON schemas

if [ $# -ne 2 ]; then
  echo "Usage: $0 <manifest or run-results> <version: v1, v2...>"
  exit 1
fi

SCHEMA="$1"
VERSION="$2"

FILENAME=$SCHEMA
CLASS_NAME=""
if [[ "${SCHEMA}" == "manifest" ]]; then
  CLASS_NAME="DbtManifest"
elif [[ "${SCHEMA}" == "run-results" ]]; then
  FILENAME="${SCHEMA//-/_}"
  CLASS_NAME="DbtRunResults"
else
  echo -e "Choose either 'manifest' or 'run-results'"
  exit  1
fi

URL=https://schemas.getdbt.com/dbt/${SCHEMA}/${VERSION}.json

OUTPUT=generated/dbt_${FILENAME}_${VERSION}.py

echo $URL
echo $OUTPUT
poetry run datamodel-codegen \
  --url ${URL} \
  --disable-timestamp \
  --class-name ${CLASS_NAME} \
  --enum-field-as-literal all \
  --input-file-type jsonschema \
  --use-title-as-name \
  --reuse-model \
  --output ${OUTPUT} \
  --output-model-type pydantic_v2.BaseModel

# Disable mypy type-checking for generated files
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' ${OUTPUT}
