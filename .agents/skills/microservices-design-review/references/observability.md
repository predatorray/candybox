# Observability

Use this file to review whether the people running the system can tell what it is doing, why it is failing, and whether users are affected.

## Monitoring vs observability

Monitoring is watching for problems you predicted: dashboards and alerts on known metrics. Observability is the ability to answer questions you didn't predict, from the data the system emits. Distributed systems fail in new ways, so a design needs both: alerts for known failure modes, and rich enough data to investigate unknown ones.

The question to ask of any design: when a user reports that a request was slow or wrong, how long will it take someone on call to find which service and why?

## Building blocks

**Aggregated logs.** All services send logs to one searchable place. Set this up before the second service ships; it is the first tool anyone reaches for.
- Use structured logs (key-value or JSON) with a common format across services: timestamp, service, instance, level, correlation ID.
- Don't log secrets or unnecessary personal data.

**Correlation IDs.** Generate an ID at the edge for each incoming request and pass it through every call, message, and log line. Without it, connecting a user's request to what happened in six services is guesswork. Add it from the start; retrofitting across many services is painful.

**Metrics.** Request rate, error rate, and latency (as percentiles, not averages) for every service and every outbound dependency, plus resource use. Tools differ in how they handle high-cardinality labels (per-customer, per-endpoint); check the chosen tool can handle the questions the team will ask.

**Distributed tracing.** Records the path and timing of a request through services, showing where time is spent and where errors start. Use an open standard for instrumentation so tools can change. Sampling keeps cost down; make sure errors and slow requests are kept.

## Are we doing OK?

**Service level objectives.** Define what "working" means per capability, from the user's point of view: for example, 99.5% of searches return in under 300 ms over 30 days. Measure the indicator, compare with the objective, and use the gap (error budget) to decide when to slow down feature work and fix reliability.

**Alert on user impact.** In a large system, some instance is always failing. Paging someone because one node died, when users aren't affected, wears out on-call people and teaches them to ignore alerts. Page on symptoms users feel (error rate, latency against SLO); use tickets or dashboards for causes that can wait.

An alert should need a human to act, and should arrive while there is still time to act. One underlying problem should produce one alert, not fifty. The message should say what is wrong, how bad it is, and link to the runbook or dashboard that helps fix it.

**Semantic monitoring.** Check that the system is doing its job, not just that processes are alive. Synthetic transactions (a test user searching and booking a fake room every minute) catch failures that health checks miss.

## Standardize

Agree on a small set of standards across services: log format, metric names, correlation ID header, trace propagation, health check endpoint. Without them every service is different and cross-service investigation is slow. A platform team can provide these through shared templates (see [organization.md](organization.md)).

## Choosing tools

Look for tools that:
- Everyone on the team can use, not only specialists, at a cost that allows wide access.
- Integrate easily and use open formats, to avoid lock-in.
- Show context: link a metric spike to traces and logs from the same time.
- Are close to real time.
- Handle the system's expected scale.

## Where to start

If a design has nothing yet, the order is roughly: aggregated logs with correlation IDs, then basic metrics per service with alerts on user-facing error rates, then tracing, then SLOs.

## Review checklist

- [ ] Logs are aggregated and structured with a common format.
- [ ] A correlation ID flows through calls, messages, and logs.
- [ ] Rate, errors, and latency percentiles exist for each service and each dependency.
- [ ] Tracing is planned for multi-service requests.
- [ ] SLOs are defined per capability from the user's view.
- [ ] Alerts page on user impact, not on every component failure.
- [ ] Synthetic transactions cover the main journeys.
- [ ] Cross-service observability standards are written down.

Related: [resilience.md](resilience.md), [delivery-and-testing.md](delivery-and-testing.md) for testing in production.
