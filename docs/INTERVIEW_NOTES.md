# Interview notes: defending the three reliability changes

## Outbox pattern
**What problem does it solve?** The dual-write problem. Saving to MongoDB and publishing to Kafka are two systems, so one can succeed and the other fail. An order could be saved but never processed.

**How does it work here?** The order is saved with `eventPublished: false`. We publish to Kafka and set the flag to `true` only after Kafka accepts it. A relay runs every 5 s and publishes any order still marked `false`.

**Why a flag on the same document instead of a separate outbox collection?** One document write in MongoDB is atomic, so "order exists" and "event still owed" are saved together with no transaction. Multi-document transactions need a replica set, which my single-node setup does not have. A separate outbox collection plus a transaction is the textbook version.

**Can an event be published twice?** Yes. If the app crashes after Kafka accepts the message but before the flag is updated, the relay resends it. That is why the consumer must be idempotent. Outbox gives at-least-once publishing, idempotency makes the result effectively once.

**Why does the relay skip very new orders?** So it does not race with the normal request path that is publishing at that exact moment. If it did race, it would only create a harmless duplicate, but skipping avoids it.

**Why polling?** It is simple and easy to reason about. Alternatives are MongoDB change streams or Debezium (change data capture), which are more efficient but add complexity.

**What is the ordering risk?** The relay can publish older orders after newer ones. That is fine here, because each order has its own key and events for one order stay ordered within a partition.

## Idempotency
**Why is it needed?** Kafka is at-least-once. The same message can be delivered twice after a consumer rebalance, a retry, or an outbox re-send.

**What does "idempotent" mean?** Processing the same message twice has the same effect as processing it once.

**How did you implement it?** An order may move from PENDING to PROCESSED/FAILED only once. I use `findOneAndUpdate({ orderId, status: 'PENDING' }, ...)`. MongoDB applies the match-and-update atomically, so if two handlers race, only one update matches. Only the winner publishes the result event.

**Why not just check the status first and then update?** That is a check-then-act race. Two handlers can both read PENDING and both proceed. The condition inside the update closes that gap.

**Is it perfect?** No. In a true race both handlers might do the (simulated) work, but only one changes the state and publishes. Real side effects like charging a card need their own idempotency key.

**Is this exactly-once?** No. It is at-least-once delivery with idempotent handling, which gives the same visible result.

## Redis fallback
**Why does the cache never block the API?** Redis is an optimisation. If it fails, the request still succeeds from MongoDB.

**What did you change?** Cache reads and writes are in try/catch, so an error becomes a cache miss. I also set `disableOfflineQueue: true`. By default the Redis client queues commands while disconnected, so requests would hang instead of failing fast.

**What about stale data?** Invalidation is best-effort, so a missed delete could leave stale data. The 10 s TTL puts an upper bound on that.

**Is caching even useful at this scale?** Honestly no, MongoDB with an index handles it easily. I implemented it to learn cache-aside, invalidation and failure handling.

## How the three fit together
Outbox makes sure the event is eventually published (possibly more than once). Idempotency makes duplicates harmless. Redis fallback keeps reads working when the cache is down. The idea in each: assume something will fail, and design so the system still ends up correct.
