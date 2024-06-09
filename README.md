# kafka-in-practice

Real-world Apache Kafka patterns, demonstrated with **JUnit 5 integration tests** that run against a real broker
started by **Testcontainers**. Read the code, run `mvn clean test`, and watch the INFO logs: lag growing,
partitions moving between consumers, requests shrinking as batching kicks in.

| | |
|---|---|
| Java | 17+ |
| Build | Maven 3.9.6+ |
| Kafka client | `org.apache.kafka:kafka-clients:3.6.0` |
| Broker (in tests) | `confluentinc/cp-kafka:7.5.2` (= Apache Kafka 3.5.x) via `org.testcontainers:kafka:1.18.3` || Tests | JUnit 5.10 |

## Run it

You need a JDK 17+, Maven, and a running **Docker** daemon (Testcontainers starts the broker for you).

```bash
mvn clean test                          # everything (~1-2 min, mostly the batching load tests)
mvn test -Dtest=ConsumerLagTest         # one class
mvn test -Dtest=ConsumerRebalanceTest#cooperativeRebalanceOnlyRevokesThePartitionsThatMove
```

The first run pulls the Kafka image. One broker is started per JVM and shared by all test classes; every test
uses its own randomly named topic and consumer group, so tests don't interfere with each other.

No Docker? Point the tests at any Kafka 3.x broker you already have:

```bash
mvn test -Dkafka.bootstrap.servers=localhost:9092
```

## Project layout

```
src/main/java/com/example/kafka/
  producer/BatchingProducer.java      producer wrapper: batch.size, linger.ms, compression.type, acks=all + idempotence
  consumer/PollLoopConsumer.java      poll loop, manual commits, rebalance listener, graceful shutdown
  admin/ConsumerLagMonitor.java       lag = log-end offset - committed offset, read through the AdminClient
src/test/java/com/example/kafka/
  KafkaTestBase.java                  shared Testcontainers broker + small helpers (topics, awaiting conditions)
  ConsumerLagTest.java                lag builds up and drains
  ConsumerRebalanceTest.java          join / leave, cooperative vs eager rebalancing, no loss, no duplicates
  BatchingLingerTest.java             linger.ms, batch.size and compression, measured
```

## What the tests show

**`ConsumerLagTest`**
- A slow consumer (~40 records/s) behind a producer that writes 300 records in milliseconds: lag is positive,
  and grows when a second burst arrives. Once the consumer is sped up, lag drains to zero and every record was
  processed exactly once.
- A consumer that keeps up stays at zero lag.

**`ConsumerRebalanceTest`**
- A second consumer joins: 4 partitions are split 2/2 and never owned twice. It leaves: the survivor gets all 4 back.
- **Cooperative-sticky** rebalancing revokes only the 2 partitions that move; **eager** (range) revokes all 4 first.
- Consumers join and leave in the middle of a 3,000-record stream: nothing is lost and nothing is processed twice,
  because offsets are committed in `onPartitionsRevoked` and on shutdown.

**`BatchingLingerTest`** (the assertions use the producer's own metrics: requests sent, records per request, bytes on the wire)
- `linger.ms=20` at ~1 record/ms: records per request go from ~1 to ~20.
- A lone record with `linger.ms=300` takes ≥300 ms to be acked: lingering is pure latency when there is nothing to batch.
- A full batch is sent immediately, even with `linger.ms=10000`.
- Under a flood of 10,000 records, tuned batching beats `batch.size=0` on throughput, request count and average ack latency.
- lz4 and gzip each cut the bytes on the wire by more than half for repetitive payloads.

## Cheat sheet

**Producer**

| Setting | Effect | Rule of thumb |
|---|---|---|
| `batch.size` | Max bytes per partition batch. Full batches are sent right away. | 64-256 KiB for high throughput |
| `linger.ms` | How long to wait for a batch to fill. | 5-20 ms is a cheap throughput win; keep 0 for latency-critical paths |
| `compression.type` | Per-batch compression, so bigger batches compress better. | `lz4` (fast) or `zstd` (smaller) |
| `acks=all` + idempotence | No loss on broker failover, no duplicates on retry. | Keep on. This project does. |

**Consumer**

| Pattern | Why |
|---|---|
| `enable.auto.commit=false`, commit after processing | At-least-once: a crash re-delivers instead of losing records |
| Commit in `onPartitionsRevoked` | The next owner continues exactly where you stopped |
| `CooperativeStickyAssignor` | Rebalances don't stop the world |
| `wakeup()` + `close()` | Interrupts a blocking poll and leaves the group immediately instead of after `session.timeout.ms` |
| One thread per `KafkaConsumer` | The consumer is not thread-safe; `wakeup()` is the only exception |

## Troubleshooting

- **`Could not find a valid Docker environment`**: start Docker, or use `-Dkafka.bootstrap.servers=...`.
- **Timing assertions fail on a very slow or heavily loaded machine**: the `linger.ms` and throughput comparisons use
  wide margins, but they are still wall-clock based. Re-run, or close other CPU-hungry processes.