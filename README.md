<img src=".github/logo.svg#gh-light-mode-only" alt="Splitter" width="300" />
<img src=".github/logo_dark.svg#gh-dark-mode-only" alt="Splitter" width="300" />

Splitter is a control-plane service for assigning work to connected clients.
Its primary focus is coordination for stateful sharded services.

## Features

- Uses UUID spaces as work domains: splits them into shards and assigns to clients.
- Emphasizes exclusive shard ownership in a steady state using leases. Optional dual ownership
  during shard transitions for advanced use cases.
- Promotes an easier programming model in comparison to coordinators that provide eventual
  consistency in shard assignments.
- Shard management is handled centrally in Splitter, using Raft for storage and coordination.
- Assumes that client instances are dynamic and can come and go.
- Region-aware assignments for multi-regional services.
- Automatic propagation of routing information to client instances.
- Can be used for leadership election by client instances.
- Provides authoritative shard alignment information for external services interested in
  arranging data and compute across multiple regions.
- Built with no external dependencies and only requires persistent storage to store metadata.

## Documentation

- [Sharding Model](./docs/sharding.md) describes motivation behind stateful services and
  explains Splitter's approach.
- [Guide](./docs/guide.md) describes Splitter model and its features.
- [Engineering blog post about Splitter](https://techblog.atoms.co/p/easy-as-pie-stateful-services-at)

## Getting Started

To see Splitter in action, check out an [example](https://github.com/atoms-co/splitter-examples/blob/main/robots/README.md)
that illustrates how to manage connected robots. Only requires Docker and Bazel.

The example shows how to integrate Splitter with Bazel projects. Splitter Go client library
can also be used in native Go projects.

### Usage

To build and load a Docker image:

```bash
bazel run //:load
```

After starting Splitter in a container, it can be controlled using `splitterctl`:

```bash
docker exec <container> /usr/local/bin/splitterctl
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

[![Build Status: Go](https://github.com/atoms-co/splitter/workflows/Go/badge.svg)](https://github.com/atoms-co/splitter/actions/workflows/go.yml)
[![Build Status: Bazel](https://github.com/atoms-co/splitter/workflows/Bazel/badge.svg)](https://github.com/atoms-co/splitter/actions/workflows/bazel.yml)
