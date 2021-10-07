#!/bin/bash
set -e

# Generate various data models for dbt manifest & catalog using official JSON schemas
MANIFEST_URL=https://schemas.getdbt.com/dbt/manifest/v2.json
CATALOG_URL=https://schemas.getdbt.com/dbt/catalog/v1.json
OUTPUT_DIR=generated

poetry run datamodel-codegen \
  --url $MANIFEST_URL \
  --class-name DbtManifest \
  --enum-field-as-literal all \
  --output $OUTPUT_DIR/dbt_manifest.py

poetry run datamodel-codegen \
  --url $CATALOG_URL \
  --class-name DbtCatalog \
  --enum-field-as-literal all \
  --output $OUTPUT_DIR/dbt_catalog.py

# Disable mypy type-checking for generated files
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $OUTPUT_DIR/dbt_catalog.py
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $OUTPUT_DIR/dbt_manifest.py
