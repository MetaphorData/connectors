#!/bin/bash
set -e

# Generate various data models for dbt manifest & catalog using official JSON schemas
MANIFEST_V1_URL=https://schemas.getdbt.com/dbt/manifest/v1.json
MANIFEST_V2_URL=https://schemas.getdbt.com/dbt/manifest/v2.json
MANIFEST_V3_URL=https://schemas.getdbt.com/dbt/manifest/v3.json
MANIFEST_V4_URL=https://schemas.getdbt.com/dbt/manifest/v4.json
CATALOG_V1_URL=https://schemas.getdbt.com/dbt/catalog/v1.json

OUTPUT_DIR=generated
MANIFEST_V1_FILE=$OUTPUT_DIR/dbt_manifest_v1.py
MANIFEST_V2_FILE=$OUTPUT_DIR/dbt_manifest_v2.py
MANIFEST_V3_FILE=$OUTPUT_DIR/dbt_manifest_v3.py
MANIFEST_V4_FILE=$OUTPUT_DIR/dbt_manifest_v4.py
CATALOG_V1_FILE=$OUTPUT_DIR/dbt_catalog_v1.py

poetry run datamodel-codegen \
  --url $MANIFEST_V1_URL \
  --class-name DbtManifest \
  --enum-field-as-literal all \
  --output $MANIFEST_V1_FILE

poetry run datamodel-codegen \
  --url $MANIFEST_V2_URL \
  --class-name DbtManifest \
  --enum-field-as-literal all \
  --output $MANIFEST_V2_FILE

poetry run datamodel-codegen \
  --url $MANIFEST_V3_URL \
  --class-name DbtManifest \
  --enum-field-as-literal all \
  --output $MANIFEST_V3_FILE

poetry run datamodel-codegen \
  --url $MANIFEST_V4_URL \
  --class-name DbtManifest \
  --enum-field-as-literal all \
  --output $MANIFEST_V4_FILE

poetry run datamodel-codegen \
  --url $CATALOG_V1_URL \
  --class-name DbtCatalog \
  --enum-field-as-literal all \
  --output $CATALOG_V1_FILE

# Disable mypy type-checking for generated files
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $MANIFEST_V1_FILE
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $MANIFEST_V2_FILE
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $MANIFEST_V3_FILE
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $MANIFEST_V4_FILE
sed -i '' '1s;^;# mypy: ignore-errors\n\n;' $CATALOG_V1_FILE
