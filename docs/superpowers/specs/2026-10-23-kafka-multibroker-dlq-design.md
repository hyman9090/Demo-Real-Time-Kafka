# Kafka Multi-Broker KRaft and DLQ Design

## Goal

Extend the local order pipeline to a multi-broker Kafka cluster with a dedicated
KRaft controller quorum, replicated order topics, and a dead-letter topic. Update
the end-to-end tutorial so a learner can migrate existing data safely, operate
the new cluster, verify replication, exercise the DLQ, and roll back without
deleting the old Kafka volume.

## Current State

- Docker Compose runs one Kafka 4.0.0 node with node ID 1 and combined
  `broker,controller` roles.
- The controller quorum is static and contains only node 1.
- `orders` and `orders-audit` have two partitions. The producer publishes each
  generated order to both topics.
- Three independent consumer groups consume `orders` or `orders-audit` and
  persist validated orders to PostgreSQL.
- Invalid records currently stop the consumer without committing the source
  offset. PostgreSQL errors are retried; exhausted retries stop without commit.
- Kafka and PostgreSQL data use named Docker volumes.

Kafka 4.0 does not support converting a static KRaft quorum to a dynamic quorum.
The new topology therefore starts as a separate cluster rather than rewriting
the existing single-node metadata quorum in place.

## Topology

The new cluster uses the pinned `apache/kafka:4.0.0` image and static KRaft
membership:

| Node IDs | Role | Purpose |
| --- | --- | --- |
| 1, 2 | broker | Store topic partitions and serve clients |
| 3, 4, 5 | controller | Maintain the three-voter KRaft metadata quorum |

All nodes use the same explicit cluster ID. Each node gets a distinct persistent
volume. Controller listeners are internal to the Compose network. Brokers expose
host listeners on `localhost:9092` and `localhost:9094`, and advertise separate
internal listeners for inter-broker traffic. Python clients bootstrap with both
host endpoints.

The controller quorum can tolerate one controller failure. Both brokers are
required for writes to topics configured with two in-sync replicas; reads and
leader election can continue from an available replica, but writes fail while
the ISR is below two.

## Topics and Replicas

Create three topics, each with two partitions and replication factor 2:

- `orders`: both partitions use broker 2 as the preferred leader and broker 1
  as follower.
- `orders-audit`: replicas span brokers 1 and 2, with preferred leaders
  distributed across the brokers.
- `orders-dlq`: replicas span brokers 1 and 2, with preferred leaders
  distributed across the brokers.

Set `min.insync.replicas=2` for these topics. Producers use `acks=all` and
idempotence. Set the internal offsets and transaction-state topic replication
factors to 2, with transaction-state minimum ISR 2. Topic creation or partition
reassignment must be explicit and verified with Kafka CLI output; the default
partition count is not a substitute for changing existing topics.

## Safe Migration

The existing Kafka cluster and the new cluster use separate storage. The old
Kafka volume is never attached to the new nodes and is not deleted by the
tutorial.

Before changing clusters:

1. Stop all producers and consumers.
2. Export every record from `orders` and `orders-audit` using a one-time
   migration utility. The archive preserves key, value, headers, timestamp,
   source topic, source partition, and source offset. Archive writes are
   completed and validated before the old cluster is stopped.
3. Create a PostgreSQL dump as an additional backup. The existing PostgreSQL
   volume remains in use by the new Compose configuration.
4. Stop the old Compose services without `down -v`.

Start the new KRaft cluster using distinct broker/controller volume names,
create the target topics, and import the archive. Imported Kafka offsets and
consumer group offsets are new; source offsets are retained only as archive
provenance. Message keys, values, headers, and timestamps are preserved.
PostgreSQL rows remain in place. Replayed records are safe because the existing
database write uses `order_id` idempotently.

Retain the old single-node Compose file and volume for rollback. Do not remove
old volumes or archive files until the new cluster, topic records, replication,
consumer groups, and PostgreSQL contents have been verified. No tutorial step
uses `docker compose down -v` or automatically deletes persisted data.

## Producer and Consumer Behavior

The producer publishes each order to both `orders` and `orders-audit`, using
`acks=all` and an idempotent producer. Bootstrap configuration lists both
brokers.

Each consumer retains its explicit topic/group settings:

- `orders-to-postgres` consumes `orders`.
- `orders-audit-1` and `orders-audit-2` independently consume `orders-audit`.

All three groups keep the existing validation, database retry, idempotent
insert, and commit-after-database-success behavior. Bootstrap configuration
lists both brokers so clients can discover the cluster if one endpoint is
unavailable.

## Dead-Letter Handling

Malformed JSON, null payloads, and order-schema validation failures are routed
to `orders-dlq`. The DLQ message value is a JSON envelope containing:

- Base64-encoded original key and value.
- Original headers and timestamp.
- Source topic, partition, and offset.
- Consumer group ID, validation error, and failure timestamp.

The consumer waits for successful DLQ delivery before committing the source
offset. If DLQ publishing fails, the consumer reports the error and leaves the
source offset uncommitted. If the source commit fails after DLQ delivery, a
retry may create another DLQ record; delivery is therefore at-least-once, not
exactly-once. Separate consumer groups can independently route the same bad
record, so group ID is part of the DLQ envelope.

Database errors remain outside the DLQ path. After the existing bounded retry
policy is exhausted, the consumer stops without committing, preserving the
valid order for later retry. This avoids treating a transient sink outage as
poison data.

## Tutorial and Code Changes

Update the tutorial's architecture, `.env` bootstrap settings, Compose example,
topic setup, producer and consumer examples, test instructions, operational
verification, troubleshooting, teardown, and completion checklist. Add
instructions for:

- Backing up Kafka records and PostgreSQL.
- Migrating records and rolling back without deleting volumes.
- Checking the three-controller quorum and both brokers.
- Verifying topic leaders, replicas, ISR, and consumer-group offsets.
- Producing a deliberately malformed message and inspecting the DLQ envelope.
- Demonstrating broker/controller failure behavior and recovery.

Update the application modules to configure both broker endpoints, producer
acknowledgements/idempotence, and commit-after-DLQ-delivery behavior. Add a
tested migration utility and focused tests for migration serialization,
validation failures, DLQ envelope construction, and DLQ/commit ordering. Preserve
the existing PostgreSQL schema and idempotent insert behavior.

Keep a separate legacy Compose file for access to the old Kafka volume during
rollback. Use a new, clearly named Kafka volume set for the new cluster; retain
the existing PostgreSQL volume name.

## Verification

1. Run focused Python unit tests and static diagnostics.
2. Validate the Compose file with `docker compose config`.
3. Start the cluster and verify three controller voters and two live brokers.
4. Verify all three topics have two partitions, replication factor 2, and ISR
   containing both brokers.
5. Verify both `orders` partition leaders prefer broker 2 and followers are on
   broker 1.
6. Export/import a fixture and compare source and target key/value/headers/
   timestamps while confirming offsets are newly assigned.
7. Inject an invalid message and verify a DLQ envelope appears before the
   source group offset advances.
8. Make DLQ publishing fail and verify the source offset does not advance.
9. Stop one broker and verify the available replica can lead reads while new
   writes are rejected with ISR below two; restart it and verify ISR recovers.
10. Confirm PostgreSQL rows survive migration and replay does not create
    duplicate `order_id` rows.
11. Confirm rollback instructions preserve both the old Kafka volume and the
    PostgreSQL volume.

## Out of Scope

- TLS, SASL, ACLs, production credentials, and public network exposure.
- Exactly-once transactions across Kafka and PostgreSQL.
- Automatic deletion of old volumes, archives, or database backups.
- Sending database connection failures to the DLQ.
- Changing the order event schema or PostgreSQL schema.
