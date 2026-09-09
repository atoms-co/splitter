#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"/..

# Formats Go imports using the Bazel-managed goimports-reviser binary.
bazel run --run_in_cwd @com_github_incu6us_goimports_reviser_v3//:v3 -- \
  -company-prefixes go.atoms.co \
  -excludes pb \
  -format \
  -rm-unused \
  -set-alias \
  -set-exit-status \
  ./...
