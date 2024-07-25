#!/usr/bin/env bash

APOLLO_IOS_CLI_VERSION=1.14.0

wget -c \
    "https://github.com/apollographql/apollo-ios/releases/download/${APOLLO_IOS_CLI_VERSION}/apollo-ios-cli.tar.gz" -O - | \
    tar -xz

./apollo-ios-cli fetch-schema --path ./apollo-codegen-config.json

rm -f ./apollo-ios-cli

poetry run ariadne-codegen --config ariadne-codegen.toml
poetry run black .
poetry run isort .