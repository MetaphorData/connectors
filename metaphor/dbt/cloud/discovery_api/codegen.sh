#!/usr/bin/env bash

# The tool is called `apollo-ios-cli`: https://www.apollographql.com/docs/ios/code-generation/codegen-cli/
# It does not mean it's iOS only.
APOLLO_IOS_CLI_VERSION=1.15.3

wget -c \
    "https://github.com/apollographql/apollo-ios/releases/download/${APOLLO_IOS_CLI_VERSION}/apollo-ios-cli.tar.gz" -O - | \
    tar -xz

./apollo-ios-cli fetch-schema --path ./apollo-codegen-config.json

rm -f ./apollo-ios-cli

poetry run ariadne-codegen --config ariadne-codegen.toml
poetry run black .
poetry run isort .