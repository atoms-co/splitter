# Model

This document describes the motivation behind stateful services and explains Splitter's approach.

## Sharded Model

The majority of distributed services are designed to be request-driven – i.e., actions are taken
only in response to a request – and stateless – i.e., service instances hold no state beyond
the request handling. Persistent state is stored externally, typically in a database, and written
and fetched on demand. More requests to the service are directly reflected in more requests to the database.
Initially this model gives developers several perceived advantages, such as scalability and simplicity, but
as complexity increases over time (with new features and demands), services become more sophisticated
and harder to scale.

An alternative to that approach is a model with internal coordination and statefulness instead
of necessarily relying on external services. Even though this approach is harder to get right,
the benefits far outweigh the development resources required to build such services. At the heart of its
difficulty is horizontal scalability.

The sharded model scales by breaking up ownership of the work domain (e.g., users, orders, stores)
into shards and dividing it among service instances. Each shard owner holds exclusive control over
certain actions, depending on the use case. For example, a service can use shard ownership to maintain
an authoritative in-memory cache backed by the usual database. By making all read and write requests
go through the shard owner, it can return cached values from memory with a global read-after-write
consistency guarantee – in contrast to the stateless design above.

This model provides low latency and correctness at scale when done well. It requires:

- a partitionable work domain,
- ownership management: assigning and distributing shard ownership,
- routing of requests to shard owners.

Solving these problems requires either creating a custom internal work distribution mechanism
(e.g., using a gossip or consensus algorithm) or relying on a generalized data store such
as etcd or Zookeeper.

## Splitter Model

To build highly resilient multi-region distributed services, Splitter offers a model with multiple tradeoffs
aimed at reliability and ergonomics:

- **Dynamic**: service instances are ephemeral. They generally do not use local physical disks and may
  come and go as the service scales up or down. There should be no special logic needed for auto-scaling
  nor regional failure. Shards should be load-balanced across whatever instances are connected, and shard
  re-assignments might be mildly disruptive at worst but fundamentally benign.
- **Multi-region**: services are deployed across multiple regions for geo-resiliency. Shard assignments should
  align well with underlying databases, where the data placement can be controlled for region-local latency.
  Shard placement decisions should not be the concern of the service, and operational controls around data
  location should be available.

The key needs of the sharded model are handled as follows:

1. **Ownership**. Domain and shard management is configured and handled centrally in Splitter, which
   internally uses Raft for distributed storage and coordination. At runtime, service instances
   establish a connection to Splitter to start receiving leased shard assignments. If an instance
   crashes or loses connectivity for too long, its lease lapses and its shards are reassigned
   to other connected instances.

2. **Routing**. Connected instances receive up-to-date shard routing information, i.e.,
   which instance owns what, that can be used for internal forwarding and fanout.
   This “routing logic as a library” approach is essential: it works for both batch
   and streaming applications, supports non-1:1 routing and leaves the service in control of the data path.

The work in Spliiter is organized in domains. A domain represents single or multiple UUID spaces divided into shards.

A shard is a half-open UUID range for a domain. This choice provides a number of advantages over discrete or
custom shard spaces (e.g., integers or strings). UUIDs force a uniform distribution that naturally avoids hotspots,
and shards can be evenly split or merged dynamically. Since services in practice never know their eventual scale
upfront as the business evolves, no-downtime re-sharding is a crucial feature. Most entity identifiers
are UUIDs as well.

## Example Use Cases

The following are examples of services built using Splitter:

- [Keyed Event Queue](https://techblog.atoms.co/p/reliable-order-processing): a multi-region,
  dynamically-scaled message broker for managing a large number of independent strictly ordered message queues.
- [Robotic Conveyance Routing](https://techblog.atoms.co/p/robotic-order-conveyance): multi-regional service to manage robots for order dispatch and pickup.
- Services with in-memory caches of rich objects.
