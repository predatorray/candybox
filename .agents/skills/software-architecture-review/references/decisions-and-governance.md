# Decisions and governance

Covers: which decisions count as architectural, how to record them, common ways decision-making goes wrong, design principles versus rules, and how to keep a structure from eroding with automated checks. Use it when reviewing ADRs, a design doc's rationale, or a team's architecture standards.

## What a design needs besides boxes

An architecture is more than its topology. Four things together describe it:

1. **Structure**: the style and the main parts.
2. **Quality attributes**: what the structure must achieve.
3. **Decisions**: hard rules about how the system is built ("only the Billing service writes to the ledger tables").
4. **Principles**: guidance that leaves room for judgment ("prefer async messaging between services unless the caller needs the result to continue").

A doc that names a style and draws boxes but gives no rules, principles or reasons is incomplete. Anyone can read how a system works from the code; nobody can read why from it.

## Which decisions are architectural

A decision is architectural when it affects one of:

- The structure or style (whether two services share data).
- A quality attribute (choosing a store because of its write throughput).
- Dependencies between parts.
- Interfaces and contracts between parts, including their versioning.
- How the system is built and run (frameworks, platforms, deployment practices) when those choices constrain the above.

A specific technology choice can be architectural if it exists to serve a quality attribute. Choosing a particular UI framework is usually not; requiring "a component-based UI framework so teams can own UI slices" might be.

Good architectural decisions guide teams rather than micromanage them. Leave implementation detail to the people writing the code unless the detail protects a quality attribute.

## Decision records

Record each significant decision in a short document of its own, a page or two. A template that works:

- **Title**: numbered, short, descriptive. "17. Shipment events go through the outbox, not direct publish."
- **Status**: proposed, accepted, superseded (with a link to the replacement, and the replacement links back). Optionally "open for comment until <date>" to avoid endless debate.
- **Context**: the situation forcing the decision and the options considered. Writing this well also documents that part of the architecture.
- **Decision**: stated firmly ("we will..."), with the reasons. Include business reasons, not only technical ones. If the decision has no business value, question whether it should be made.
- **Consequences**: what gets better, what gets worse, and the trade-offs accepted. This is where future readers learn that someone already considered the objection they are about to raise.
- **Compliance**: how adherence will be checked, manually or by an automated test, and where that test lives.
- **Notes**: author, approver, dates, change history.

Optionally add an alternatives section with each option analyzed.

Store records in one place that everyone who needs them can reach, one record per file or page. A repository next to the code works for application-level decisions; decisions spanning many systems often fit better in a shared wiki. Notify people by linking to the record, not by pasting the decision into chat or email, which creates copies that drift.

Agree up front which decisions an individual can approve and which need a wider review. Common triggers for wider review: cost above a threshold, impact on other teams, and security implications.

Superseded records stay. The history of why a choice was made and then reversed stops the same debate from restarting.

## How decision-making goes wrong

Watch for these patterns, in the doc and in the team's history:

- **Deferring out of fear.** Decisions keep getting postponed in case they are wrong. Decide at the last point where you have enough information without blocking the team, and stay close to implementation so you can adjust when reality disagrees.
- **Relitigating.** The same decision is argued again every few months because nobody wrote down why it was made. Fix with records that include reasons and consequences.
- **Decisions nobody can find.** The decision exists in a meeting, a chat thread or someone's inbox, so new people never learn it and violate it. Fix with one place of record and notifications that link to it.
- **Reversing a decision without knowing its purpose.** Example: a team removes the synchronous stock check from checkout and replaces it with an async event "to decouple Checkout from Inventory", and overselling starts on popular items. The sync check had been chosen to prevent exactly that, with the coupling accepted as the price. A record would have said so.

## Standards with reasons

Teams follow rules they understand. Write standards in the same format as decisions: context, the rule, why it exists, what it costs. If you cannot write a convincing why, drop the rule. When developers ask to add a library or a new language, ask two things: does something in the system already do this, and what is the technical and business case? The second question often settles the matter on its own.

## Keeping the structure from eroding

Rules that are not checked decay. Code review catches some violations, but late and inconsistently. Automate the checks that matter, the same way tests check behavior. Any mechanism that objectively measures an architectural property can serve:

- **Structure tests** in the build: no dependency cycles between packages; the UI layer may not import persistence; module A may only use module B's public API; plug-ins may not depend on each other. Tools exist for most languages (for example ArchUnit for the JVM, NetArchTest for .NET, dependency-cruiser for JavaScript, import-linter for Python).
- **Metric thresholds** in CI: complexity per function, module size, coupling counts. Fail or warn when a threshold is crossed.
- **Performance budgets** in CI or staging: p95 latency for key endpoints, page weight, startup time.
- **Production checks**: monitors for SLOs, scanners for misconfigured ports or missing auth, cleanup jobs that find services nobody calls, controlled fault injection to verify the system survives instance or zone loss.

Treat these as a checklist the system runs on itself, not as heavy process. Explain each check to the team before enforcing it; a check people do not understand gets disabled.

## Keep the architecture under review

Architectures age. A design that fit three years ago may no longer fit the business or the technology. Also check the delivery pipeline: a well-modularized system with a four-week test cycle and quarterly releases is not agile, whatever its diagram says.

## Red flags

- No recorded rationale for the main structural choices.
- Decisions that exist only in chat or meeting notes.
- Standards with no stated reason.
- A record whose consequences section lists only benefits.
- Layer or module rules stated in a doc but not enforced anywhere.
- No automated check for dependency cycles in a codebase meant to be modular.
- A proposal to reverse an earlier decision that does not mention why it was made.
- An architect or lead who owns critical-path code and has become a bottleneck for the team.
