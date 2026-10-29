# Event-driven designs

Covers: broker (choreographed) versus mediator (orchestrated) topologies, events versus commands, async error handling, where messages get lost, request-reply over messaging, and when to use events at all. Use it for any design built on queues, topics, streams or workflow engines.

## Request-driven or event-driven

A request asks the system for something specific and expects an answer: "show this account's last 50 invoices". An event reports that something happened and lets interested parts react: "trial account created". Requests fit well-structured, deterministic work where you need control and certainty over the result. Events fit work where many parts react to the same fact, where load comes in bursts, or where the response to the user does not need to wait for all processing.

Most real systems mix the two. Flag designs that force one model onto everything.

## Broker topology (choreography)

Producers publish events to topics on a broker. Processors subscribe, do their piece, and publish a new event describing what they did. Nobody controls the whole flow.

Example: `trial-account-created` is consumed in parallel by provisioning (creates the tenant), notifications (sends a welcome email), CRM sync, and billing (starts the trial clock). Provisioning later publishes `tenant-provisioned`, which onboarding listens to.

Good practice: every processor publishes what it did even if nobody listens yet. Then a new consumer (say, an analytics service) can be added without touching any existing service.

Strengths: very loose coupling between processors, high throughput, each processor scales on its own, and the topic buffers load when a consumer falls behind.

Weaknesses to review:

- **No one knows when the business transaction is done.** There is no owner of the overall state.
- **Errors are invisible to everyone else.** If billing crashes on one event, provisioning and email still proceed as if everything worked. The account is live but no trial clock is running.
- **No restart.** You cannot simply replay the initiating event, because some steps already happened and are not idempotent.
- **Inconsistent intermediate states** are normal and must be acceptable to the business.

## Mediator topology (orchestration)

An initiating event goes to a mediator that knows the steps. It sends commands to dedicated queues for each processor, waits for replies, and moves to the next step. Steps can run in parallel where order does not matter.

Strengths: the mediator holds workflow state, so it can handle errors, pause, persist where it stopped, and resume (for example, wait for a failed payment to be fixed, then continue from fulfillment). Consistency is easier to reason about.

Weaknesses: processors are more coupled to the mediator, the mediator must scale and stay available (it is on every flow), and throughput is usually lower. Complex dynamic flows are hard to model declaratively, so many systems use a mediator for the main path and broker-style events for side reactions.

Pick the mediator tool by workflow complexity. Code-defined routing in an integration library suits simple flows. A workflow or state-machine engine suits flows with many branches and compensation. Flows that wait on humans for hours or days (manual approval above a threshold) need a long-running workflow engine with durable state. Using a heavy engine for simple flows wastes months; hand-coding complex long-running flows produces fragile code. One approach is to route every event through a simple mediator that hands harder classes of workflow to a more capable engine.

Choose between the two topologies by asking which you need more: control over workflow, errors and recovery (mediator), or throughput and decoupling (broker).

## Events versus commands

An event is a past-tense fact (`payment-captured`); consumers may ignore it. A command is an instruction (`capture-payment`); exactly one handler must process it. Mixing them causes confusion: a "notification" that some service silently depends on is really a command and needs delivery guarantees and an owner. Check that names and delivery guarantees match intent.

## Async error handling

When processing is async, there is no caller waiting to receive an error. Design where failures go:

- A consumer that hits a bad message should not block the queue while it figures things out. Move the message aside (to a repair service or dead-letter queue) and keep consuming.
- A repair step can fix known problems automatically (strip a stray unit from a numeric field, fill a default) and resubmit. Unfixable messages go to a human-facing queue or console where someone can correct and resubmit them.
- Resubmitted messages arrive out of order. If order matters within a key (all updates for one account must apply in sequence), hold later messages for that key until the failed one is resolved, or partition by key so one consumer owns the order.
- The user still needs to learn about the failure. If they were told "accepted", decide how they hear about a later rejection (email, in-app status, a pending state in the UI).

## Where messages get lost

Walk the path from producer to final store and check each hop:

1. **Producer to broker.** The send fails silently, or the broker accepts it in memory and crashes. Fix: durable queues or topics, and a send that waits for the broker's acknowledgment that the message is persisted. Also consider writing the event to the producer's database in the same transaction as the state change and relaying it afterward (an outbox), so a crash between "save" and "publish" does not drop it.
2. **Broker to consumer.** The consumer takes the message and crashes before processing it. Fix: acknowledge only after processing, so the broker redelivers.
3. **Consumer to its database.** The write fails after the message is acknowledged. Fix: acknowledge only after the database commit, and make processing idempotent so redelivery after a crash between commit and ack is harmless.

At-least-once delivery plus idempotent consumers is the usual safe combination. Treat any claim of "exactly once" as a question: exactly once where, under which failures?

## Request-reply over messaging

When a caller needs an answer (an order number, a confirmation code) but the system is message-based, use a request queue and a reply queue. Two common ways to match replies to requests:

- **Correlation ID**: the caller sends a message with an ID and waits on a shared reply queue for a message carrying that ID. Scales well.
- **Per-request temporary reply queue**: simpler, but creating and deleting a queue per request strains the broker at high volume.

Either way, the caller and responder are now tied together operationally for the length of the wait: if the responder is down, the caller is stuck. Treat request-reply links like synchronous calls when judging availability.

## Broadcast

Publishing to a topic with unknown subscribers is the loosest coupling available, and the basis for many consistency and analytics patterns. The cost: you lose track of who depends on the data. Keep a registry of consumers per topic, and treat the event schema as a public contract (`distributed-tradeoffs.md`).

## Strengths and limits overall

Event-driven designs are strong on throughput, responsiveness, spike absorption and adding new consumers. They are weak on testability (the number of possible event orderings explodes), debuggability, guaranteed outcomes, and simplicity. Hybrids are common: services or a monolith that publish events for side effects while keeping core writes synchronous.

Also note: two event processors that share a database, or that use request-reply, are still one operational unit, even though they communicate asynchronously.

## Red flags

- Broker-style choreography for a flow that needs a guaranteed outcome and recovery, with no owner of its state.
- No dead-letter or repair path.
- Auto-acknowledge on receipt.
- Publishing an event after a database commit in separate steps with no outbox or equivalent.
- Non-idempotent consumers on an at-least-once broker.
- Ordering assumed globally when the broker only guarantees it per partition or queue.
- Commands disguised as events.
- Event-driven reads that a user waits on.
- "Exactly once" asserted without a mechanism.
