# Encoding, schema evolution, and dataflow between services

Use this when the design defines message formats, API contracts, stored record layouts, or a deploy process where old and new code run side by side.

## The core problem

Code and data change at different speeds. During a rolling deploy, old and new versions of a service run at the same time. Clients on users' phones may be months behind. Data written five years ago is still in the database. So every format must handle two directions:

- **Backward compatible**: new code can read data written by old code. Usually easy: new code knows the old format.
- **Forward compatible**: old code can read data written by new code. Harder: old code must ignore what it does not understand, and must not destroy it.

A design that changes formats without saying how both directions work will break during its first deploy.

## Choosing an encoding

**Language-native serialization** (Java serialization, pickle, and similar). Avoid for anything stored or sent between services. Ties you to one language, versioning is an afterthought, and decoding arbitrary bytes can let an attacker run code.

**Text formats (JSON, XML, CSV).** Human-readable and universal. Weaknesses: numbers are ambiguous (large integers lose precision in parsers that use floating point), no native binary type (base64 adds a third), schemas are optional and often skipped, verbose on the wire. Fine for public APIs and low-volume traffic; check the large-integer case for IDs.

**Binary schema-based formats (Protobuf, Thrift, Avro, and similar).** Compact, fast, and the schema doubles as documentation. Compatibility rules are explicit:

- Fields identified by tag numbers (Protobuf, Thrift): you can add fields with new tags, and old code skips unknown tags. Never reuse a tag. New fields must be optional or have defaults. Removing a field is only safe if it was optional, and its tag must be retired.
- Fields matched by name with writer and reader schemas resolved at read time (Avro): the reader needs to know the writer's schema. Systems store a schema version ID with each record or file, often in a schema registry. Add or remove only fields that have defaults.
- Changing a field's type is risky in all of them; widening (int32 to int64) may work one way and truncate the other.

A schema registry with compatibility checks in CI is the cheapest way to stop breaking changes.

## The field-loss trap

Old code reads a record that has a new field, decodes it into an object that lacks that field, modifies something else, and writes it back. The new field is gone. This happens in databases and in services that proxy or update messages. Check that libraries preserve unknown fields, or that old code never writes back records it did not fully understand.

## How data flows, and what each path needs

**Through a database.** The writer encodes, a future reader decodes. Data outlives code, so backward compatibility must hold across many versions. Migrating every row on every schema change is expensive; most systems add nullable columns or defaulted fields and leave old rows alone until read. Large rewrites belong in a background backfill, not in the deploy.

**Through service calls (REST, RPC).** Assume servers are updated before clients, so requests need backward compatibility and responses need forward compatibility. Public APIs need explicit versioning because you do not control the clients. Remote calls are not local function calls, whatever the RPC framework suggests:

- They can time out with no way to know whether the server did the work.
- Retrying is only safe if the operation is idempotent (see `integration-and-correctness.md`).
- Latency varies widely.
- Arguments must be encoded; passing large objects or references is not free.

Look for: timeouts on every call, retry with backoff and jitter, idempotency keys on non-idempotent writes, and circuit breakers.

**Through asynchronous messages (queues, logs, actors).** The broker buffers when consumers are slow or down, redelivers after crashes, and decouples senders from receivers. Costs: one-way by default (replies need a separate channel), delivery can be duplicated or reordered, and consumers on mixed versions read the same topic. Message schemas need the same compatibility rules as stored data, because messages can sit in a topic for days. See `stream-processing.md`.

## Rolling out a breaking change safely

The pattern is expand, migrate, contract:

1. Expand: add the new field or table alongside the old. New code writes both, reads old.
2. Migrate: backfill existing data. Switch reads to the new field once backfill is verified.
3. Contract: stop writing the old field, then remove it after every reader (including batch jobs and analytics) has moved off.

Each step must be deployable and reversible on its own.

## Red flags

- JSON IDs that exceed 2^53 with JavaScript or other float-based consumers.
- No schema for messages that cross team boundaries.
- Reused or renumbered field tags; required fields added to an existing schema.
- A deploy plan that assumes all nodes switch versions at once.
- Code that reads a record into a typed object and writes the whole object back.
- RPC calls without timeouts, or retries on non-idempotent operations.
- A "big bang" schema migration that locks a large table during deploy.
- Analytics or downstream consumers left out of the compatibility plan.
