#!/usr/bin/env bash

# Updates Go files generated from protobufs using Bazel

set -euo pipefail

cd "$(dirname "$0")"/..

# Location
bazel build //proto/atoms/splitter/lib/service/location:location
cp bazel-bin/proto/atoms/splitter/lib/service/location/location_/go.atoms.co/splitter/lib/service/location/pb/location.pb.go lib/service/location/pb
chmod +w lib/service/location/pb/*

# Session
bazel build //proto/atoms/splitter/lib/service/session:session
cp bazel-bin/proto/atoms/splitter/lib/service/session/session_/go.atoms.co/splitter/lib/service/session/pb/session.pb.go lib/service/session/pb
chmod +w lib/service/session/pb/*

# Splitter
bazel build //proto/atoms/splitter:splitter
cp bazel-bin/proto/atoms/splitter/splitter_/go.atoms.co/splitter/pb/*.go pb
chmod +w pb/*

# Splitter private
bazel build //proto/atoms/splitter/private:private
cp bazel-bin/proto/atoms/splitter/private/private_/go.atoms.co/splitter/pb/private/*.go pb/private
chmod +w pb/private/*
