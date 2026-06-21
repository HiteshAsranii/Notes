# Apache Kafka Notes

## Kafka Basics

### Topic
- A Topic is a logical category or stream of events/messages.
- One Topic can have multiple Partitions.

```text
Topic
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
```

### Kafka
- Distributed Event Streaming Platform.
- Stores events in partitions.
- Supports high throughput and scalability.

---

## Partitions & Offsets

### Event Storage
- Events are written sequentially.
- Each event is assigned an Offset.
- Offsets are maintained independently per partition.

Example:

```text
Partition P1:
Offset 0 -> E0
Offset 1 -> E3

Partition P2:
Offset 0 -> E1
Offset 1 -> E4

Partition P3:
Offset 0 -> E2
```

### Important Notes
- Ordering is guaranteed only within a partition.
- Events are read sequentially using increasing offsets.
- Global ordering across partitions is **not maintained**.
- Kafka maintains **append-only logs**.

---

# Kafka Log Segments

Each partition is stored as log files.

```text
Topic
 └── Partition 0
      ├── Segment 0 (Offset 0 - 500)
      └── Segment 1 (Offset 500 - 900)
```

## Why Segments?

Instead of keeping one huge log file:

- Easier indexing.
- Faster reads.
- Easier cleanup and retention.
- Better disk management.

### Example

If a consumer wants to read from offset `800`:

- Kafka identifies the segment containing offset `800`.
- Reads from that segment directly.
- Similar to indexing in databases.

---

## Segment Index

Each segment maintains an index.

```text
Segment
 ├── Data File
 └── Index File
```

### Notes

- Every offset is not stored in the index.
- Index entries are sparse.
- Kafka stores index entries after a configurable number of bytes.
- Used to quickly locate offsets inside the log.

---

# Kafka Controller

The Controller is responsible for cluster management.

## Responsibilities

- Topic creation.
- Leader election.
- Follower assignment.
- Broker failure detection.
- Notify brokers about metadata changes.

---

## Controller Node

A controller is:

```text
Normal Broker
      +
Controller Role
```

Only one broker acts as controller at a time.

---

## Cluster Metadata Log

Kafka maintains cluster metadata.

### Topic Creation Flow

```text
Producer/Admin CLI
        |
        v
    Any Broker
        |
        v
   Controller
        |
        +--> Create Topic
        +--> Elect Leader
        +--> Assign Followers
        +--> Update Metadata Log
```

---

## KRaft Consensus

Modern Kafka uses KRaft (Kafka Raft) instead of ZooKeeper.

Responsibilities:

- Metadata management.
- Controller election.
- Consensus between controller nodes.
- Cluster state replication.

```text
KRaft Consensus
        |
    Controller
    /   |   \
  B1   B2   B3
```

---

# Leaders and Followers

Each partition has:

- One Leader
- One or more Followers

### Leader

- Handles Reads and Writes.
- Maintains partition log.

### Follower

- Replicates leader's log.
- Takes over if leader fails.

```text
Leader
  |
  +--> Read Requests
  +--> Write Requests

Followers
  |
  +--> Replicate Data
```

---

# Partition Assignment

When publishing events, Kafka must determine which partition receives the message.

Example:

```java
partition = hash(orderId) % numberOfPartitions
```

For 3 partitions:

```text
hash(orderId) % 3
```

Possible result:

```text
P0
P1
P2
```

### Benefits

- Same key always goes to same partition.
- Ordering maintained for the same key.

Example:

```text
Order 101 -> P1
Order 101 Update -> P1
Order 101 Payment -> P1
```

---

# Consumer Offsets

Consumers maintain their own offsets.

Kafka does not track processing progress for every consumer individually.

Example:

```java
partition = hash(groupId) % partitions
```

Consumer Groups store offsets separately.

---

## Why Producer Needs Metadata?

When sending data:

1. Producer calculates target partition.
2. Producer must know which broker is leader for that partition.
3. Message is sent directly to the partition leader.

Example:

```text
Producer
    |
    v
Metadata
    |
    v
Partition P1 Leader = Broker B2
    |
    v
Send Message to B2
```

---

# Kafka Cluster Example

```text
Cluster

Broker B1
 ├── Topic T1
 │    ├── P0
 │    └── P1
 └── Topic T2
      ├── P0
      └── P1

Broker B2

Broker B3

Consumer Group 1
Consumer Group 2
```

---

# Leader/Follower Example

Topic: `orders`

```text
Partitions: 3
Replication Factor: 2
```

Total Partition Copies:

```text
3 Partitions × 2 Replicas = 6 Copies
```

### Distribution

Broker B1

```text
P0 Leader
P1 Follower
```

Broker B2

```text
P1 Leader
P2 Follower
```

Broker B3

```text
P2 Leader
P0 Follower
```

### Summary

```text
Partition P0
  Leader   -> B1
  Follower -> B3

Partition P1
  Leader   -> B2
  Follower -> B1

Partition P2
  Leader   -> B3
  Follower -> B2
```

This distribution balances load and provides fault tolerance.

---

# Key Interview Points

1. Ordering is guaranteed only within a partition.
2. Offsets are partition-specific.
3. Kafka uses append-only logs.
4. Producers write to partition leaders.
5. Followers replicate leader data.
6. Consumer groups maintain offsets.
7. KRaft replaces ZooKeeper.
8. Partitions enable scalability.
9. Segments improve storage and lookup efficiency.
10. Partition selection is commonly based on hashing a message key.




---

# Controller High Availability & KRaft

## Problem Statement

How do we ensure robustness if the controller fails?

### Legacy Approach - ZooKeeper

```text
Kafka
  |
ZooKeeper
```

Characteristics:

- Separate deployment.
- Separate setup and management.
- Uses ZAB Consensus Protocol.
- Additional operational complexity.

---

## Modern Approach - KRaft

KRaft = Kafka + Raft

### Advantages

- Preferred modern architecture.
- No external dependency (ZooKeeper removed).
- Metadata stored inside Kafka.
- Uses Raft Consensus Algorithm.

---

## Why Consensus?

Only one controller can actively manage metadata.

Responsibilities:

- Topic creation.
- Partition assignment.
- Leader election.
- Metadata updates.

Since a single controller is critical, Kafka uses consensus to ensure failover.

---

# Raft Consensus

### Controller Roles

```text
1 Active Controller
N Standby Controllers
```

Example:

```text
Controller 1 -> Active

Controller 2 -> Standby
Controller 3 -> Standby
```

If the active controller fails:

- Standby controllers elect a new leader.
- Cluster continues operating.

---

## Quorum

Raft requires majority agreement.

Formula:

```text
Quorum = (N / 2) + 1
```

Example:

```text
3 Controllers

(3 / 2) + 1 = 2
```

Minimum 2 controllers must agree.

---

## Metadata Changes

Whenever metadata changes:

Examples:

- Topic creation.
- Topic deletion.
- Partition reassignment.
- Broker join/leave.

The active controller:

1. Writes metadata change.
2. Replicates to quorum.
3. Commits only after quorum acknowledgement.

---

# ISR (In-Sync Replicas)

ISR = In-Sync Replica Set

Replicas that are sufficiently caught up with the leader.

---

## Example

Replication Factor = 3

```text
Broker1 -> Leader
Broker2 -> Follower
Broker3 -> Follower
```

Suppose:

```text
Broker3 falls behind
```

Then:

```text
ISR = {Broker1, Broker2}
```

Broker3 is removed from ISR.

---

## How Kafka Determines ISR Membership

A replica is considered in-sync when:

### Offset Condition

Replica offset is close to leader's latest offset.

```text
Leader Offset = 1000
Replica Offset = 999
```

Still considered in sync.

---

### Time Condition

Follower must not exceed:

```text
replica.lag.time.max.ms
```

If exceeded:

```text
Replica removed from ISR
```

---

## ISR Update

```text
Leader
  |
  +--> Monitor Followers
  |
  +--> Add to ISR
  +--> Remove from ISR
```

Only the leader manages ISR membership.

---

# Producer Acknowledgements (ACK)

Producer acknowledgement modes:

```text
acks=0
acks=1
acks=all
```

---

## acks = 0

Fire and Forget

```text
Producer
   |
   +--> Send Message
```

Producer does not wait for response.

### Characteristics

- Lowest latency.
- Highest throughput.
- Possible data loss.

---

## acks = 1

Wait for Leader

```text
Producer
   |
 Leader Writes
   |
 ACK Returned
```

Characteristics:

- Faster than `all`.
- Data may be lost if leader crashes before replication.

---

## acks = all

Wait for ISR Replication

```text
Producer
      |
      v
 Leader
      |
      v
 All ISR Replicas
      |
      v
 ACK Returned
```

Characteristics:

- Highest durability.
- Higher latency.
- Recommended for critical data.

---

# ISR and Durability

When using:

```text
acks=all
```

Kafka waits for all ISR members to replicate data before acknowledging success.

Example:

```text
ISR = {Broker1, Broker2}
```

Success occurs only when:

```text
Broker1 -> Write Complete
Broker2 -> Replication Complete
```

---

## Minimum In-Sync Replicas

Configuration:

```properties
min.insync.replicas=2
```

Meaning:

At least 2 ISR members must be available.

---

### Example

```text
Replication Factor = 3
min.insync.replicas = 2
```

Valid:

```text
ISR = {B1, B2}
```

Invalid:

```text
ISR = {B1}
```

Result:

```text
Writes are rejected
```

This prevents acknowledged data loss.

---

## Important Interview Point

ISR can never effectively drop below the configured durability requirements.

If:

```text
ISR Count < min.insync.replicas
```

Then:

```text
Partition becomes unavailable for writes
```

(when using `acks=all`)

---

# Kafka Log Retention Policies

Kafka supports two primary cleanup strategies.

---

## 1. Delete Policy (Default)

Old data is physically removed.

Configuration:

```properties
cleanup.policy=delete
```

Controlled by:

```properties
retention.ms
retention.bytes
```

---

### Time-Based Retention

Example:

```text
Retention = 7 Days

Segment 1 -> 8 Days Old -> Delete
Segment 2 -> 6 Days Old -> Keep
```

---

### Size-Based Retention

Example:

```text
Retention Size = 1 GB
```

If log exceeds:

```text
1 GB
```

Oldest segments are removed.

---

## 2. Compact Policy

Configuration:

```properties
cleanup.policy=compact
```

Kafka retains only the latest value for each key.

---

### Example

```text
User1 -> Version 1
User1 -> Version 2
User1 -> Version 3
```

After compaction:

```text
User1 -> Version 3
```

Older records removed.

---

### Common Use Cases

- User profiles.
- Configuration data.
- Customer state.
- CDC (Change Data Capture).

---

# Page Cache Optimization

One reason Kafka achieves high throughput is its use of the OS Page Cache.

---

## Write Path

```text
Producer
   |
Broker
   |
OS Page Cache (RAM)
   |
Async Flush
   |
Disk
```

---

## Process

1. Broker receives data.
2. Data written to OS Page Cache.
3. Operating system keeps data in memory.
4. Data asynchronously flushed to disk.

---

## Benefits

### Faster Writes

Writing to RAM is much faster than disk.

---

### Faster Reads

Frequently accessed data remains in cache.

```text
Consumer
   |
Page Cache
   |
Response
```

Disk access often avoided.

---

### Zero-Copy Optimization

Kafka can leverage:

```text
sendfile()
```

to transfer data directly from page cache to network socket.

Benefits:

- Reduced CPU usage.
- Reduced memory copies.
- Higher throughput.

---

# Kafka Reliability Summary

| Feature | Purpose |
|----------|----------|
| KRaft | Controller consensus and metadata management |
| Quorum | Majority agreement for metadata changes |
| ISR | Tracks healthy replicas |
| acks=0 | Maximum throughput |
| acks=1 | Leader durability only |
| acks=all | Strong durability |
| min.insync.replicas | Prevent acknowledged data loss |
| Delete Policy | Remove old segments |
| Compact Policy | Keep latest value per key |
| Page Cache | High-performance I/O |

# Interview Questions

### What happens if the Kafka controller fails?

- In KRaft mode, standby controllers elect a new active controller using Raft consensus.

### What is ISR?

- Set of replicas that are sufficiently caught up with the leader.

### Difference between replication factor and ISR?

- Replication Factor = total copies.
- ISR = healthy synchronized replicas.

### Difference between acks=1 and acks=all?

- `acks=1` waits only for leader.
- `acks=all` waits for all ISR replicas.

### What happens if ISR falls below min.insync.replicas?

- Kafka rejects writes when using `acks=all`.

### Why is Kafka fast?

- Sequential disk writes.
- Append-only logs.
- Page cache usage.
- Zero-copy transfers.
- Partition-based parallelism.
