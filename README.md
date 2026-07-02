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


# Kafka Notes

---

# Failure Scenarios

### 1)
- Reprocessing of events when **consumer crashes before committing the offset**.

### 2)
- Consumer crashes or takes long for processing.

Diagram (concept):

```
OE

+---------+          +---------+
|   P1    | <------  |   C1    |
|   P2    |          |   C2    |
+---------+          +---------+
    B1

                    +---------+
                    |   C     |
                    |   C2    |
                    +---------+
                        B2
                  (offsets managed here)
```

- C fails or takes long for heartbeat.
- B2 removes C from there.
- Assigns C2 to consume events which might already be there in the queue.
- C tries to commit → **fails**.

---

# Cluster Setup

- `bin`
- `config`

### Roles

- Controller
- Broker (Controller + Broker)

### ID
- Unique ID

### Listeners

Internally opens a new socket server connection to these ports.

```
CONTROLLER://:9093
BROKER://:9092
```

### controller.listener.names

- Which node to choose in order to communicate with other nodes.

Suppose you mention:

```
CONTROLLER
```

and you have above two options,

then it will use the **9093** one.

---

### quorum.voters

```properties
quorum.voters=1@localhost:9093
```

### listener.security.protocol.map

```properties
CONTROLLER:PLAINTEXT
```

Also supports:

- SSL/TLS

### logs.dirs

```properties
/tmp/c1.logs
```

### num.partitions

```properties
3
```

### default.replication.factor

```properties
2
```

Similar configuration is for Brokers.

In broker we need to specify:

- listeners
    - Ports on which our Producer/Consumer
      (Spring Boot / Node applications)
      will connect.

Other broker configurations:

- offset replication factor
- log segment size

---

# Generate Cluster ID

### Step 1

```bash
bin/kafka-storage.sh random-uuid
```

Output:

```
xyz...
```

Then:

```bash
bin/kafka-storage.sh format \
-t <output> \
-c config/controller.properties
```

Same for **all nodes**.

This will create:

```
meta.properties
```

---

### Step 2

Start all nodes

1. Controllers
2. Brokers

```bash
kafka-server-start.sh <properties-path>
```

---

Now it will create **Cluster Metadata**

Inside it we have:

- quorum-state
    - showing the voting process for
      leader & follower.

Now we can check the cluster status.

We cannot directly contact controllers,
so we connect to any broker first.

Create a topic.

Then check the status.

You'll get:

- partitions
- replicas
- leaders
- etc.

---

# Producer Flow

### Case 1

Partition provided.

### Case 2

Key is provided.

### Case 3

No key / No partition

Old:

- Round Robin

New approach:

- Sticky Partitioning

---

Flow

```
Serialization
      ↓
Partition
      ↓
Accumulator Append
      ↓
Compression
      ↓
Sender Thread
```

---

### Record Accumulator

Application thread is free after accumulator.

Accumulator is an in-memory buffer of Kafka.

It:

- Collects events topic partition wise.
- Groups them into batches.

Compression:

- Snappy
- Gzip

Topic partition-wise batches ensure broker doesn't need to connect & append repeatedly.

Sender thread:

- Looks for batches.
- Finds leader broker for partition.
- Sends them.

---

# Round Robin vs Sticky Partitioning

### Round Robin

Batch sizes may become very small,
eventually leading to multiple network calls.

### Sticky Partitioning

Unlike Round Robin,

events of a certain topic stick to a single partition,

resulting in much larger batch sizes.

Record Accumulator keeps track of this.

---

### Batch Size

Example:

```
1 event = 1 KB
```

### Linger.ms

Batch building duration.

Example:

```
10 ms
```

Wait for 10 ms,

accumulate all records,

then send.

After accumulation,
messages are compressed.

---

# Max Inflight Connections

Sender Thread

Default:

```
5
```

If:

```
max.retry > 0
```

and

```
inflight.connections > 1
```

Problems:

1. Reconsumption of event.
2. Order interchanged.

Solution:

```properties
enable.idempotence=true
```

---

## Producer ID (PID)

Producer requests a unique PID from Kafka Cluster.

Now in Record Accumulator every batch header contains:

1. PID
2. Base Sequence
3. Last Sequence

Topic partition-wise sequence.

Each event gets a sequence number.

Example:

```
Event 1 → seq 0
Event 2 → seq 1
Event 3 → seq 2
```

Even if retried,
same sequence number is used.

Example:

```
Base Seq : 0
Last Seq : 2
```

---

# Kafka Listener

If we take Java scenario,

using:

```java
@KafkaListener
```

By default:

- accepts one event
- creates one consumer
- listens to that event.

If one consumer should consume multiple events,

use:

```java
@KafkaListener
```

and pass a list of topics.

---

In `application.properties`

Deserializer:

```
StringDeserializer
```

Then conditionally typecast it to that event.

Producer:

```java
KafkaTemplate<String, Object>
```

When Object is used,

Kafka sends by default a

```
__TypeId__
```

inside event header.

Example:

```
__TypeId__ = com.eda.producer.model.Order
```

---

# Producer → Consumer Mapping

We can provide mapping.

Example:

```
com.producer.Order
        ↓
com.consumer.Order
```

Or

provide trusted packages and a list for deserializers.

```
Object
```

---

Example

```
Topic 1 : Order

Producer(P0)
      ↓
Consumer Group 1
   C1
   C2

Topic 2 : Producer

Producer(P0)
      ↓
Consumer Group 1
```

---

If multiple listeners are needed for a single event,

create one consumer using:

```java
@KafkaListener
```

It initializes one listener.

In `application.properties`

```properties
kafka.listener.concurrency=3
```

Container Factory manages it automatically.


---

# Exception Handling in Consumers

Possible exceptions:

- Exception during deserialization.
- Exception while processing the record.

Consumer flow:

```
Poll
  ↓
Deserialize   (1)
  ↓
Process       (2)
  ↓
Commit Offset
```

---

## 1. Deserialization Failure (Poison Pill)

If deserialization fails:

- Not able to process the record.
- Offset is not committed.
- Endless loop of consuming the same buggy message (Poison Pill).

### Solution

Use:

- Deserialization Wrapper

Send failed events to:

- DLT (Dead Letter Topic)
    - Failed events are stored here.

Default behavior:

- Don't retry.
- Log it.
- Commit offset.
- Move to next record.

Retry strategies can be configured using Beans.

Available strategies:

- FixedBackOff
- ExponentialBackOff

Recovery:

- `DeadLetterPublishingRecoverer`

Example DLT topic:

```
order-event-dlt
```

> (Important)

---

# Consumer Assignment Strategies

Supported scenarios:

✔ One consumer → Multiple topics

✔ Multiple consumers in a group → Different topics

✔ Multiple consumers → Same topic

---

## Assignment Strategies

(Default)

- Range Assignor
    - Divides and distributes partitions.

Other options:

- Round Robin Assignor
- Sticky Assignor

---

## Eager Rebalancer

During rebalance:

- Consumer stops reading.

---

## Cooperative Sticky Assignor

- Tries to stick to existing partition assignments.
- Allows read operations to continue while minimizing partition movement.

Partition assignment strategy is configured in:

```
application.properties
```

---

# Consumer Fetch Configuration

Consumer fetch request settings:

- Fetch request data size
- Poll frequency
- Minimum timeout
- Maximum timeout
- Fetch bytes

Example:

```properties
max.poll.records=500
```

Suppose:

One poll fetches:

```
2000 records
```

Flow:

- Consumer keeps all records in its internal buffer.
- Processes 500 records.
- Commits offsets.
- Polls again.

No additional network call is made until the local buffer becomes empty.


---

# Transactions in Kafka

Each Producer must have a **unique Transaction ID**.

- Multiple producer instances → Different Transaction IDs.

Configured in:

```properties
application.properties
```

Possible naming strategies:

```text
order-{serverPort}
```

or

```text
order-{hostname}
```

Avoid:

```text
{UUID}
```

because every restart generates a new Transaction ID, which is **not acceptable**.

---

# Transaction Coordinator

Kafka internally maintains a topic:

```
__transaction_state
```

It:

- Stores transaction information.
- Has around **50 partitions**.
- Is distributed across brokers.

---

## Producer Initialization

Producer first finds the **Transaction Coordinator** by requesting any broker.

Then Kafka assigns a:

- Producer ID (PID) (Idempotency)

The Transaction Coordinator writes metadata into the appropriate partition of:

```
__transaction_state
```

Partition is selected using hashing.

Example entry:

```text
TID  = "order-service-0"
PID  = 5     (received from Kafka)
Epoch = 0
State = EMPTY
Partitions Involved = [ ]
```

---

## Why do we need Transaction ID when Kafka already assigns a PID?

Reason:

Every time the producer/server restarts,

the **PID may change**.

If a stable **Transaction ID (TID)** exists,

Kafka can associate the producer with the same logical producer and restore the correct Producer ID.

---

# Zombie Fencing

Suppose:

Producer 1 starts a transaction.

Assigned:

```
TID
PID
State
```

Example:

```
TID1
State → Ongoing
```

Now Producer 2 takes over Producer 1.

It uses the same:

```
TID1
```

Kafka detects:

- An ongoing transaction already exists.

So it:

- Aborts the previous transaction.
- Increments the Epoch.

Now if Producer 1 revives,

it still has:

```
TID1
PID = 5
Epoch = 0
```

But Kafka already increased the Epoch to:

```
Epoch = 1
```

Therefore,

Producer 1 becomes a **Zombie Producer**,

and Kafka rejects its requests.

---

## Epoch

Epoch is basically a **version number**.

Every restart of the producer increases the Epoch for that:

- Transaction ID
- Producer ID

---

## Kubernetes

Kubernetes can inject a stable Transaction ID into producer instances,

so application code usually doesn't need to manage it manually.

---

# Transaction States

State flow:

```
EMPTY
   ↓
ONGOING
   ├── Prepare Commit
   │        ↓
   │      COMMIT
   │
   └── Prepare Abort
            ↓
          ABORT
```

These are rigid states.

Once a transaction reaches:

- Prepare Commit
- Prepare Abort

it **cannot return** to the ONGOING state.

Even if the Transaction Coordinator crashes,

after restart it must continue from the stored state.

The Transaction Coordinator stores:

- Transaction ID (TID)
- Producer ID (PID)
- Current State
- Partitions involved
- Timeout
- Epoch

All maintained **partition-wise** inside `__transaction_state`.

Transactions are **single-threaded**.

---

# Outbox Pattern

Suppose:

```
Place Order
    ↓
Save to DB
    ↓
Send Kafka Event
```

One of these operations may fail.

Examples:

- Order saved but event not published.
- Event published but order not saved.

This is known as the **Dual Write Problem**.

---

## Approach 1 — @Transactional + try/catch

```
@Transactional
```

Catch:

Event may actually be published,

but acknowledgement may not be received.

Application rolls back the database transaction,

while Kafka has already published the event.

This creates inconsistency.

---

## Approach 2 — Two Phase Commit (2PC)

Idea:

Coordinate DB and Kafka commits together.

However,

**Kafka does not support traditional Two Phase Commit (2PC).**

---

# Two Phase Protocol

Flow:

```
ONGOING

      ├── Prepare Commit
      │         ↓
      │      Commit
      │
      └── Prepare Abort
                ↓
              Abort
```

Once transaction reaches Prepare stage,

it cannot move back to:

```
ONGOING
```

Even if the Transaction Coordinator fails,

after restart it must continue from the current state.

---

# Recommended Pattern

Instead of publishing directly to Kafka,

first write everything to the database.

Flow:

```
Write to DB
      ↓
Read from DB
      ↓
Publish to Kafka
```

Database tables:

- Order Table
- Outbox Table

Outbox stores the events waiting to be published.

A separate process reads these records.

Possible implementations:

- Poller
- CDC (Change Data Capture)

---

## Example Outbox Table

| id | aggregate_type | event | agg_id | payload |
|----|----------------|-------|--------|----------|
|101|Order|order-event|Created At|Published|

---

# Poller Approach

Multiple poller instances may lead to:

- Duplicate processing
- Ordering risk
- Shared locking

Other drawbacks:

- Constant database load
- Database cleanup required
- Duplicate publishing

Therefore,

the Poller should handle **Idempotency**.

---

# CDC (Debezium)

Instead of polling,

the database itself informs about changes.

Debezium reads database changes using:

## WAL (Write Ahead Logging)

Used by:

- PostgreSQL

MySQL equivalent:

- Binary Log (Binlog)

WAL also helps in:

- Data recovery.

Replication level:

- Logical

Logical replication stores metadata about the changes we want.

Typical flow:

1. Create a table.
2. Create a Publication for specific tables.
3. Subscribe only to that Publication.

---

## Kafka Connect

Debezium works using:

```
Kafka Connect
```

Kafka Connect is part of the Kafka ecosystem and is used to integrate external systems like databases with Kafka.
