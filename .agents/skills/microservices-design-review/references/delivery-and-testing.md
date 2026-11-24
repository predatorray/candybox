# Build, deployment, and testing

Use this file to review whether each service can be built, tested, deployed, and released on its own, and whether the test strategy fits a distributed system.

## Continuous integration

Check that CI is real, not just a build server:
- Engineers merge to the main line at least daily.
- Every merge runs an automated test suite.
- A broken build is fixed first, before anything else.

Long-lived feature branches delay integration and make merges risky. Prefer short-lived branches or trunk-based development, with unfinished work hidden behind toggles.

## Pipelines and artifacts

- **One pipeline per service**, triggerable on its own. If changing one service requires building all of them, independent deployment is gone.
- **Build the artifact once**, then promote the same artifact through each environment. Rebuilding per environment means production runs something that was never tested.
- **Keep environment config outside the artifact.** Same image, different configuration per environment.
- **Fast feedback first.** Order stages so the quick, likely-to-fail checks run early.
- **Fewer environments where possible.** Each extra environment between commit and production adds delay and cost. Each one should earn its place by catching something the others don't.

## Repository layout

| Layout | Strengths | Risks |
|---|---|---|
| One repo per service | Clear ownership, natural per-service builds and permissions | Changes across services need several PRs; shared code needs packaging and versioning |
| Monorepo | Atomic changes across services, easy code reuse, one place to search | Ownership can blur; build tooling must build only what changed; easy to create hidden coupling between services |
| One repo, one build for everything | None worth having at scale | Every change rebuilds and redeploys everything |

Either of the first two can work. What a review should check is that each service has its own build trigger and deployable artifact, that ownership of each directory or repo is explicit, and that cross-service changes are rare. If cross-service changes are frequent, the boundaries are the problem, not the repo layout.

## Deployment

### Principles

- **Isolated execution.** Each service instance gets its own resources, so one service can't starve another. Containers give cheap isolation; many services on one shared VM do not.
- **Automate everything.** With many services, manual steps don't scale and introduce errors.
- **Infrastructure as code.** Environments are defined in version-controlled config and can be rebuilt from scratch.
- **Zero-downtime deploys.** Rolling or blue-green deploys, with health checks and graceful shutdown (drain connections, finish in-flight work).
- **Desired state.** Declare how many instances should run and let the platform keep it that way.

### Platform options

| Option | When it fits | Watch for |
|---|---|---|
| VMs | Existing ops tooling, few services | Slow provisioning; one service per VM is costly |
| Containers on an orchestrator (e.g., Kubernetes) | Many services, need for density and standard deploys | Large operational burden. Not worth it for a handful of services. Prefer a managed offering. |
| Platform as a service | Standard web workloads, small teams | Less control; may not fit unusual needs |
| Functions as a service | Spiky or event-driven work; teams that want minimal ops | Cold starts, execution limits, harder local testing. Deploying a whole service as one function is a fine starting point. |

Recommend the simplest platform that meets the needs. A team running four services rarely needs to operate its own cluster.

### Database changes

Each service owns its schema migrations. Schema changes must work with both the old and new versions of the code while a rolling deploy is in progress: add columns first, deploy code that uses them, remove old columns later.

## Separate deployment from release

Deploying puts new code in production. Releasing exposes it to users. Keeping these separate lowers risk:

- **Feature toggles:** deploy code switched off, turn it on for some users, turn it off if something goes wrong. Remove old toggles; they accumulate as debt.
- **Canary release:** send a small share of traffic to the new version, compare error rates and latency with the old, then widen or roll back. Automate the comparison where possible.
- **Parallel run:** run old and new side by side on the same input and compare outputs before trusting the new one.
- **Blue-green:** bring up the new version alongside the old and switch traffic in one step, with instant switch-back.

A design that can only release by deploying, with rollback by redeploying, is slower to recover.

## Testing

### Scope

- **Unit tests:** fast, many, test one piece of logic. The bulk of the suite.
- **Service tests:** test one service through its API, with dependencies stubbed. Catch integration bugs inside the service without needing other services.
- **End-to-end tests:** run several real services together. Highest confidence per test, but slow, flaky, and expensive.

Keep most tests at the cheaper scopes. Smart stub servers that mimic dependencies' APIs make service tests realistic.

### Problems with end-to-end tests across services

- **Flaky.** More moving parts, more random failures. Teams start ignoring red builds. Remove or fix flaky tests immediately.
- **Unclear ownership.** A shared suite owned by nobody, or by a separate QA team, decays. Each test needs an owner.
- **Slow, so changes pile up.** Long runs make teams batch changes, which makes each release bigger and riskier.
- **They encourage lockstep releases.** If the suite certifies a specific set of service versions together, teams start releasing that set as one unit, which ends independent deployment.

### What to use instead

- **Consumer-driven contract tests.** Each consumer states, as an executable test, what it expects from a provider's API. The provider runs all consumers' contracts in its own pipeline. A provider change that would break a consumer fails the provider's build, before deploy, without running both services together.
- **Schema compatibility checks** for APIs and events, run in CI.
- **Testing in production:** synthetic transactions (fake users making real requests and checking results), canary analysis, parallel runs. These catch problems that only show up with real data and real infrastructure.
- **Optimize recovery as well as prevention.** No preproduction testing catches everything. Fast detection and rollback can do more for users than one more test stage.

Keep a small number of end-to-end tests for the most important journeys if they help, owned by the teams involved and kept fast.

### Cross-functional tests

Run performance and load tests early, not just before launch, because results can change the architecture. Test resilience too: kill a dependency in a test environment and check the degraded behavior matches the design.

### Developer experience

Ask how an engineer works on one service day to day. They should be able to run their service locally, with stubs or a small set of real dependencies, and get fast feedback. If they need the whole system running, productivity will drop as the system grows.

## Review checklist

- [ ] Each service has its own pipeline and artifact.
- [ ] Artifacts are built once and promoted.
- [ ] The repo layout keeps ownership explicit.
- [ ] The deployment platform is proportionate to the number of services.
- [ ] Deploys are zero-downtime, with schema changes compatible across versions.
- [ ] Release is separate from deploy (toggles, canary, or similar).
- [ ] Contract tests or schema checks guard service interfaces.
- [ ] End-to-end tests are few, owned, and not a gate that bundles releases.
- [ ] There is a plan for testing in production and for fast rollback.
- [ ] Engineers can develop one service without running everything.

Related: [communication.md](communication.md) for interface evolution, [observability.md](observability.md) for production checks.
