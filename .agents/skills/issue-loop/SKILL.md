---
name: issue-loop
description: Continuously dispatch requester-authored GitHub issues to isolated resolve-issue workers, judge their PRs independently, and approve after checks pass. Use when asked to run or resume the issue resolution queue.
---

# Continuous issue queue

Invoke as `$issue-loop` (or `$issue-loop REQUESTER_LOGIN`). This is an agent
workflow, not a background daemon. It runs while the session has budget; invoke
it again after interruption. Do not start a code audit or file speculative issues.
The coordinator selects, delegates, judges and approves; it does not implement fixes.

## Scope and defaults

- Read `AGENTS.md`. Confirm the repository from `origin` and the requester from
  the authenticated GitHub profile; if the user names a different requester,
  use that login. Persist repo and requester in every checkpoint. Do not infer
  requester from an issue author, assignee or repository owner. Stop if identity
  cannot be established. Only **issues authored by this requester** are eligible,
  including follow-ups filed under that identity. Never dispatch another author's
  issue, even when linked by an eligible issue. Recheck authorship in the worker.
- Use connected GitHub tools or authenticated `gh`; paginate every issues, PRs,
  comments, reviews and checks listing. API failure or incomplete pagination is
  an unknown state, not an empty queue. Do not proceed on incomplete evidence.
- Default: two concurrent workers, three fix/review rounds per issue (including
  judge-requested revisions), and one final judge per revision. Reduce concurrency
  to fit available agent slots. If delegation is unavailable, stop with a checkpoint;
  never substitute coordinator implementation or self-review.
- Approval is authorized by invoking this skill. **Merging is not.** Do not merge,
  enable auto-merge, close issues, or change branch protection.

## 1. Reconcile before selecting

On every start/resume, enumerate open requester issues, **all authors' open PRs**
(including drafts), and remote `issue-claims/*` and `resolve-issue/*` branches.
Inspect issue timeline cross-references and PR closing references, bodies and head
branches, plus issue comments for manual fixes. Match exact issue/repository
references, not substrings (`#12` is not `#123`). A related PR is not necessarily
an actual fix: inspect its scope. Include noncanonical human PR branches.

For each candidate, classify:

| Evidence | Action |
| --- | --- |
| Open fix PR, regardless of author or draft state | Reconcile that PR; never open a second one |
| Live claim/worker or active manual fix | Skip; report its owner and link |
| Claim with no PR after an interruption | Recover only using the handoff below; otherwise mark blocked |
| Merged fix PR with issue still open | Verify evidence; report, do not refix or close automatically |
| Needs-input checkpoint | Skip until requester answers or explicitly requests retry |
| Closed unmerged PR or closed issue | Inspect disposition; do not automatically retry rejected work |
| No ongoing work or fix | Eligible for a new claim |

Review already-open queue PRs before dispatching new work, so a restart also
finishes pending judging/approval. A claim does not prevent reconciliation of its
existing PR. Only mutate PRs clearly owned by this workflow; report human-owned
PRs unless the requester explicitly asks to take them over.

## 2. Rank and atomically claim

Prefer bugs, then testing gaps, improvements, then documentation; oldest first
within each class. Use labels/type plus title/body, explain ambiguous classification.
An older issue may be promoted to prevent starvation. Re-query between batches so
new requester follow-ups join the queue. Do not spend tokens optimizing the ranking.

Optional helper `python3 .agents/skills/issue-loop/scripts/queue.py rank --requester LOGIN` consumes stdin JSON
`{"issues": [GitHub REST issue objects]}` and emits only open requester issues,
ordered by label/title class and creation date. It is a ranking aid, **not** an
eligibility check; run the reconciliation above before dispatching anything.

Immediately before dispatch, reread the issue, linked work and remote refs. Claim
it using `python3 .agents/skills/issue-loop/scripts/queue.py claim NUMBER --base origin/main` after fetching the
actual default branch (substitute it for `main`). The helper creates a unique
empty commit and **atomically creates an absent remote ref**
`refs/heads/issue-claims/NUMBER`; only the successful claimant may dispatch.
The returned SHA is the claim token. A failed/uncertain push means stop and read
the remote ref; dispatch only if it equals your unique token. Never treat a
comment, label, assignment, local lock or a read-then-write as a distributed lock.
If git is unavailable, use an API operation that creates a ref and rejects an
existing ref (HTTP 422); do not use an upsert-style branch tool for claims.

Claims never expire and are never automatically deleted, reset or stolen. This
trades occasional blocked work for avoiding duplicate fixes when a session stops.
It coordinates agents using this protocol; humans and other automation can ignore
it, hence the mandatory open-PR recheck immediately before PR creation.

## 3. Delegate, checkpoint, repeat

Create a separate git worktree and `resolve-issue/NUMBER` branch from the fetched
default branch for each worker; never let workers share a checkout. Pass the worker
only repo, requester, issue, claim token, worktree, round budget, and invocation
`$resolve-issue NUMBER`. The worker loads that skill from the repo. Do not give it
an implementation or conclusions. Main session keeps only a small dispatch table.

Before spawning a worker, post a durable comment on the issue; update with new comments at
each transition. Use marker `<!-- issue-loop:v1 -->` and include:

- repository, requester, issue, claim SHA, coordinator/worker session IDs;
- state: `claimed`, `working`, `reviewing`, `ready`, `needs-input`,
  `approval-blocked`, `approved` or `done`;
- canonical work branch, pushed head SHA, worktree (advisory), PR URL;
- rounds used/limit, last test commands/results, unresolved findings;
- reviewer/judge verdict and exact reviewed SHA, next action or precise question.

Remote refs/PR heads are authoritative; comments are recovery notes, not locks.
Push useful commits at milestones and before budget exhaustion. Never place
credentials in checkpoints. If checkpoint publication fails, stop new dispatch
and report the claim SHA so recovery is possible. Keep blocked issues out of the
current run's candidate set; advance to the next eligible one.

### Recovery and handoff

Never reclaim based on age, silence, missing local worktrees or a clock timeout.
A resumed session first reconciles the canonical PR/branch and claim. It may
continue the same worker only when that session is demonstrably stopped (the
runtime confirms termination, or the requester explicitly confirms all prior
sessions stopped). Otherwise report an orphaned claim and continue other issues.
Record the handoff with the same claim token, old/new session IDs, current remote
head and remaining rounds **before** resuming. Reuse the branch and PR; never
create a fresh claim or reset the round count. If notes are missing, inspect
history/reviews; ambiguous ownership or round count needs requester input.
A closed unmerged PR needs an explicit retry decision; reuse/reopen it when
possible, and link its successor if a replacement is explicitly authorized.

## 4. Independent final judge

When a worker returns ready (or needs-input after its bounded loop), spawn a
**fresh agent with no inherited conversation**, separate from worker and its
reviewers. Give it only issue/PR links, repository guidance and request to judge
current code, requirements, tests and unresolved review threads independently.
It must inspect the actual diff and return `pass`, `revise` or `needs-input`, with
head SHA, evidence and blocking findings. It must not implement changes or approve.
For needs-input without a PR, give it the issue and durable checkpoint/branch;
it should validate the blocker rather than invent a PR verdict.

A revise verdict goes back to the worker within the **same three-round total**;
a new head requires another fresh judge. At the cap, record needs-input with a
specific decision needed, keep incomplete PRs draft, and continue other issues.
Do not automatically retry the same blocker each polling cycle.

## 5. Approval gate and continuation

For a workflow-owned PR, coordinator approves only when:

1. Worker completed and an independent final judge passed the **current head SHA**.
2. PR is open, ready, nonconflicting, and all blocking findings/threads addressed.
3. Query both check runs (all pages/latest attempts) and commit status contexts for
   that SHA, repository required checks/rules, and pending external PR reviewers.
   Every applicable checker must finish successfully; required checks must exist.
   Failure, pending, cancelled, timed out, action-required, unknown/missing required
   checks or unreadable policy blocks approval. Neutral/skipped counts only when
   explicitly acceptable under repository policy; no checks is not proof of success.
4. Reread head and check state immediately before submitting the GitHub APPROVE
   review, targeting that commit SHA when supported. If head changed, invalidate
   judge/check evidence. After submission verify the review's commit matches the
   current head; record a mismatch as blocked and rerun the gate.

GitHub prohibits self-approval. If the authenticated approver is the PR author,
record `approval-blocked` and request an authorized independent human/account.
Do not switch identities, forge an approval comment, or claim success. The judge
agent's independence does not create a second GitHub identity. Likewise record
insufficient permissions rather than repeatedly retrying. Continue the queue.

Leave approved PRs open for the requester to merge. Existing unmerged PRs continue
to exclude their issues. Poll pending checks with bounded waits, continuing other
eligible work. End when the queue is empty, all remaining work is blocked, user
stops, or budget is low; do not busy-loop forever. Before ending, persist all
active states and report PR links, approved/blocked status, claims needing handoff,
and the command `$issue-loop REQUESTER_LOGIN` to resume.
