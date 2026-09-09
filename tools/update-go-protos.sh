#!/usr/bin/env bash

# Updates Go files generated from protobufs using Bazel

set -euo pipefail

cd "$(dirname "$0")"/..

# Splitter
bazel build //proto/atoms/splitter:splitter_go_proto
cp bazel-bin/proto/atoms/splitter/splitter_go_proto_/go.atoms.co/splitter/pb/*.go pb
chmod +w pb/*

# Splitter private
bazel build //proto/atoms/splitter/private:private_go_proto
cp bazel-bin/proto/atoms/splitter/private/private_go_proto_/go.atoms.co/splitter/pb/private/*.go pb/private
chmod +w pb/private/*
