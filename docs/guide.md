# Splitter Guide

## What is Splitter?

Splitter is a multi-region control-plane service for assigning work to connected clients.
It uses range-based sharding of the UUID spaces and offers exclusive ownership of shard assignments
with leases, load balancing and routing information for ownership discovery.

To learn more about motivation behind Splitter read [sharding model](sharding.md).

## Overview

Work in Splitter is organized in domains. A domain represents single or multiple UUID
spaces divided into shards. A single client (consumer) may use multiple domains
(grouped as a **service** in Splitter configuration).

A **shard** is a half-open UUID range for a domain.

There are three types of domains:

- _Global_: a single UUID space divided into multiple shards.
- _Regional_: multiple UUID spaces divided into shards. Each region where a service is
  deployed is assigned a single UUID space.
- _Unit_: a singleton shard of a single UUID space. Used for leader election in certain
  low-scale and advanced use cases.

Client services connect to Splitter and register themselves using a service identifier listed
in the Splitter configuration. A service coordinator on the Splitter side includes connected
client instances (consumers) in the work distribution process. It assigns shards to the
consumers and moves them around if necessary (e.g., when a consumer is shutting down).
Consumers are receiving information about other consumers to be able to connect
to the rest of the cluster.

## Work Distribution Process

On the Splitter side (coordinator) the work distribution process involves:

- creating and maintaining leases for shards,
- assigning shards with their leases (grants) to connected consumers,
- load balancing assignments to avoid imbalance in consumers load,
- sending consumers information about shard assignments (cluster map).

Connected consumers maintain heartbeat with service coordinator; when it fails, the
consumer is considered disconnected. If it cannot re-connect in time to renew leases
for shards assigned to it, the coordinator considers the consumer left the distribution
process and reassigns its shards to other connected consumers.

Consumers do the following:

- receive assigned grants and handle their lifecycle: e.g., load state from DB,
  handle incoming requests, initiate periodic actions, etc.,
- use cluster map (received from Splitter coordinator) to initiate or forward requests
  to shards on other instances.

## Grant Lifecycle

**Grant** is a shard with an attached lease. Grants and assigned shards are used
interchangeably throughout this guide.

In a steady state a shard can only be assigned to a single consumer.

Originally the Splitter model supported only exclusive ownership of shards, where only
a single consumer ever owned a shard. This approach has a drawback of having gaps
in shard ownership during shard movements, leading to unavailability of shards.

To address this inefficiency, Splitter extended the grant lifecycle model with relaxed
guarantees of shard ownership, but only during shard movements. This extension
is optional and consumers are free to implement handling of shards assuming
exclusive ownership. On the other hand, consumers that need to minimize gaps in
shard ownership should take special care when implementing grant handling around
shard movements to prevent unwanted dual ownership.

A grant can have the following states:

- _New_ (implicit): grant is created and not yet assigned. This state is temporary,
  Splitter tries to assign a grant right after its creation.
- _Active_: shard is assigned to a single consumer and no other owners exist.
- _Revoked_: Splitter coordinator decided to move a shard from a consumer; it changed
  grant state to revoked. After this, the lease of this grant will not be extended.
  The consumer has time until the end of the lease to finish the work and release
  the grant (either explicitly before the lease is expired or implicitly at the end
  of the lease).
- _Allocated_: grant in this state is created for a shard that has a corresponding
  grant in a revoked state, at the same time when the state of the old grant is
  changed from active to a revoked state. Allocated grant is never assigned to
  the consumer with the revoked grant.

Splitter also supports two supplemental states that are produced by the consumers,
not the Splitter coordinator, to communicate phases in shard transitions:

- _Loaded_: consumer can use this state to signal the old shard owner that it
  finished loading state (e.g., from DB). The old owner can then finish handling
  the grant, knowing that state is loaded and can be handled elsewhere.
- _Unloaded_: consumers use this state to signal the new owner of a shard that
  the state is offloaded from memory (e.g., to DB). The new owner can start
  loading state after receiving notification about unloading.

Consumers can use these two states to continue serving some requests:
for example, old owner can continue to serve read-only requests while
the new owner is loading the state.

## Inter-cluster Communication

Consumers that use peer-to-peer communication (e.g., forwarding public requests)
need to provide endpoint information as part of the registration process.
These endpoints typically use IP addresses for pod-to-pod communication, and
they often serve APIs that are different from APIs of public endpoints.

Splitter client library provides components that help with implementing forwarding
logic, e.g., automatically re-trying requests to handle cases when shards are moving.

## Placements

Apart from the work distribution process, Splitter allows managing the arrangement of
UUID ranges across regions. These arrangements are called **placements**.

Placements can be used to manage the distribution of UUID ranges for domains.
They can also be used by third party services to arrange their data in alignment
with shards of Splitter consumers.

The primary purpose of placements is alignment of data and compute, which helps
with avoiding cross-regional calls and associated costs (latency and traffic).
Placements can also be used to migrate data from one region to another at
a steady pace, without bursts in traffic.
