---
name: resolve-issue
description: Resolve a GitHub issue end to end — verify it, fix it with tests, open a draft PR, drive CI and independent reviews to green, then link the PR on the issue. Use when given an issue number or link.
---

# Resolve a GitHub issue

Run this workflow only when the user explicitly asks to resolve an issue. Use the issue number or URL supplied by the user; an issue number refers to this repository. If no target issue is supplied, ask for it before proceeding.

Use the GitHub integration available in the current agent environment, or the authenticated `gh` CLI. Map the operations below to the available tools; do not assume a particular tool name.

## 1. Verify
Read the issue and its comments. Check whether it is real.
- Bug: write a failing test that reproduces it before touching the code.
- Architectural improvement: re-evaluate whether it still makes sense against the current code and `DESIGN.md`.
- Not real or already fixed: comment on the issue with the evidence and stop; tell the user.

## 2. Implement
- Compare the approaches proposed in the issue, pick the one that makes the most sense, and implement it on a properly named side branch (e.g. `fix/<short-topic>`). If the session mandates a specific branch, use that instead.
- Follow `AGENTS.md` (module boundaries, record format versioning, test conventions: no mocking frameworks, update the `*ContractTest`/`*ContractIT` suites when SPI behavior changes).
- Keep existing tests passing; update them only if the change fundamentally alters their expectations. Add new tests that guard against regression.
- Run `mvn test`, plus `mvn verify` (or the relevant `-Dit.test=...`) when engine, server or integration behavior is touched.

## 3. Open a draft PR
- One-line commit messages, no body paragraph.
- Open the PR as a **draft**, first line of the description `Fixes #<issue>`, then a few high-level paragraphs. No file-by-file or line-by-line walkthrough.
- Wait for the checks. Fix failures properly (never skip or disable tests). If checks don't trigger because the PR is a draft, mark it ready.

## 4. Independent review loop
Once all checks pass, use the following independent review loop. If agent delegation is unavailable, request an independent human review and report that the review loop is pending; do not claim a self-review is independent.

Once an independent reviewer is available:
1. Spawn a **fresh** reviewer using the current environment's agent delegation capability (independent context, not a fork). Give it the PR link, the issue link, and the review instructions below; do not pass this session's implementation rationale or conclusions. Ask it to review the PR independently and post its review and comments on the PR.
   - Read the repository's `AGENTS.md` and the three local review skills: [software-architecture-review](../software-architecture-review/SKILL.md), [data-system-design-review](../data-system-design-review/SKILL.md), and [microservices-design-review](../microservices-design-review/SKILL.md). Include their repository paths (`.agents/skills/<skill-name>/SKILL.md`) in the reviewer handoff so they can be located from the PR checkout.
   - Apply `software-architecture-review` to module boundaries, coupling, and architectural trade-offs; `data-system-design-review` to storage, replication, partitioning, consistency, and failure behavior; and `microservices-design-review` to service boundaries, inter-service contracts, resilience, and deployment behavior. Follow the skills' workflows and load their bundled references as relevant to the changed code. Keep the review proportional to the PR and state when a review area is not applicable.
   - Check the implementation and tests against the issue, `AGENTS.md`, and `DESIGN.md`. Report concrete findings with severity, code locations, failure scenarios, and suggested fixes; distinguish blocking issues from non-blocking suggestions.
2. In this session, read the comments left (by the reviewer and anyone else). Fix the ones that make sense; reply with a reason to the ones you won't fix. Keep checks green after each push.
3. After each new revision, spawn another fresh reviewer and repeat.
4. Stop when only non-blocking or nitpick comments remain and all checks pass.
5. If review goes back and forth for more than 3 rounds, stop and ask the user for a judgement.

## 5. Wrap up
- Comment on the original issue with the PR link and a high-level summary of the fix. Publish the PR if it is still a draft.
- Do **not** close or resolve the issue and do **not** approve or merge the PR; the user does that.
- Tell the user it's done, with the PR link.

## Style
Code comments, docs, commit messages and PR text are concise. Don't explain what the code already says; comment only when necessary.

## Unrelated findings
If you hit a separate problem unrelated to this issue, don't go deep. Search existing issues first, then file a follow-up issue summarizing everything you found (symptoms, location, repro hints, suspected cause) so another agent can understand, verify and fix it independently without this conversation.
