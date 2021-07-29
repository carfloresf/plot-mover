#!/usr/bin/env bash

set -o errexit
set -o nounset

golangci-lint run ./...

echo "no linting problems found"