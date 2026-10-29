---
name: resolve-issue
description: Resolve one GitHub issue with a restart-safe claim, isolated worktree, bounded fix/review loop and a single review-ready PR. Use with an issue number or URL, directly or as an issue-loop worker.
---

# Resolve one issue

Invoke as `$resolve-issue ISSUE_NUMBER` (or an issue URL in this repository).
An explicit coordinator dispatch authorizes this skill as well as a direct user
request. If no issue is supplied, ask for one. Use connected GitHub tools or
an authenticated CLI; read `AGENTS.md` and
[the queue protocol](../issue-loop/SKILL.md), especially reconciliation, atomic
claims, checkpoints and recovery. Never approve, merge or close the issue here.

## 1. Establish ownership and verify

Read the issue, all comments and linked PRs. In queue mode verify that its author
matches the requester's login supplied by the coordinator. Direct invocation may
resolve the specifically requested issue regardless of author, but must still
claim and de-duplicate. Do not expand scope to other users' issues.

Verify the supplied remote claim SHA. Without a supplied claim, reconcile existing
work and acquire one using the queue helper before editing. An existing open fix
PR means reuse only after an authorized handoff; otherwise report it and stop.
Do not assume an old claim is abandoned. Use one isolated worktree and canonical
`resolve-issue/NUMBER` branch; preserve existing uncommitted work and other agents'
checkouts. Record the claim, branch, session, round budget and state on the issue.

Verify the report against the current default branch. For bugs, reproduce with a
failing regression test before the fix where practical. For improvements, reassess
against `DESIGN.md`; for docs, compare the claim with code. If already fixed,
invalid or unclear, checkpoint evidence as needs-input and stop; do not close it.

## 2. Fix and review (at most three rounds total)

A round is one implementation/revision plus its independent review. Persist the
round counter before starting each round; restarts and final-judge revisions do
not reset it. Default limit is three, including coordinator judge feedback.

1. Implement the smallest complete fix. Follow module boundaries, versioned
   formats and testing conventions in `AGENTS.md`. Add meaningful regression
   coverage, update shared contracts when SPI behavior changes, and keep tests
   enabled. Use concise one-line commit messages.
2. Run checks appropriate to the touched code. Java changes normally require
   `mvn test`; engine/server/integration changes also require relevant `mvn verify`
   integration coverage, installing sibling test-jars first as needed. Frontend,
   scripts, charts and docs use their own relevant checks. Report exact commands,
   outcomes and unavailable dependencies. Do not claim an unrun check passed.
3. Push to the canonical branch and recheck **all authors'** open fix PRs and issue
   references immediately before opening a PR. Create exactly one draft PR with
   `Fixes #NUMBER`, problem/result and validation. If one appeared meanwhile, stop
   and reconcile; do not open a duplicate. An uncertain create response requires
   rereading PRs before retrying. Reuse the same PR on every revision and restart.
4. Fetch CI and external review feedback. Fix failures without skipping tests.
   Spawn a fresh reviewer with no inherited conversation, giving only issue/PR
   links and repository guidance. Ask it to inspect the diff, run relevant checks
   and return blocking findings with the reviewed head SHA. If delegation is
   unavailable, checkpoint needs-input; self-review is not independent review.
5. Address valid findings, explain declined ones, checkpoint used rounds and
   rerun affected checks after pushes. A new revision consumes the next round and
   invalidates prior review evidence. Exit when no blockers remain, or at the cap.

Do not wait indefinitely for infrastructure. A persistent CI/dependency failure,
unclear requirement, unavailable independent reviewer or exhausted budget yields
needs-input with a precise blocker and durable progress. Push useful checkpoints
before stopping. Do not mark a broken PR ready just to trigger CI unless the
repository requires it; document that exception and restore draft on blockage.

## 3. Return a durable result

On success, mark the PR ready and record `ready` on the issue with PR URL, head
SHA, claim, tests, review verdict and rounds used. Return these same fields to the
coordinator. It spawns a separate final judge and handles checks and squash-merging
with a title-only commit message.
For a standalone invocation, request independent final judgement from a fresh
agent and leave merging to the requester unless they invoke `$issue-loop` to
authorize coordinator merging. The worker never approves or merges its own PR.

On blockage, keep incomplete PRs draft, checkpoint `needs-input`, exact unresolved
findings and the decision needed; return the branch/PR link and remaining rounds.
A coordinator may ask its independent judge to assess the blocker. Never report
completion merely because a PR exists. Never release/delete the persistent claim.

## Unrelated findings

Do not fix unrelated problems. Search open/closed issues and PRs first; file a
narrow follow-up only when not already covered, with evidence/reproduction and
existing type labels (bug, testing, improvement or documentation as applicable).
Use the authenticated requester identity; if it differs from the queue requester,
report the finding to the coordinator rather than filing as another author.
Do not claim or resolve the follow-up here: the coordinator will discover it.
