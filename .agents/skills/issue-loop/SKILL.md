---
name: issue-loop
description: Continuously dispatch requester-authored GitHub issues to isolated resolve-issue workers, judge their PRs independently, and squash-merge after checks pass. Use when asked to run or resume the issue resolution queue.
---

# Continuous issue queue

Invoke as `$issue-loop` (or `$issue-loop REQUESTER_LOGIN`). This is an agent
workflow, not a background daemon. It runs while the session has budget; invoke
it again after interruption. Do not start a code audit or file speculative issues.
The coordinator selects, delegates, judges and squash-merges; it does not implement fixes.

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
- Invoking this skill authorizes the coordinator to **squash and merge** eligible
  workflow-owned PRs after the gate below. Do not submit an approval review or
  invent an approval requirement. Use the requester's available merge permissions,
  including admin permissions; respect enforced repository rules. Do not change
  branch protection, enable auto-merge, or close issues manually. GitHub may close
  linked issues automatically when their fixing PR merges.

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
finishes pending judging/merging. A claim does not prevent reconciliation of its
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
  `merging`, `merge-blocked`, `merged` or `done`;
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
head SHA, evidence and blocking findings. It must not implement changes, approve or merge.
For needs-input without a PR, give it the issue and durable checkpoint/branch;
it should validate the blocker rather than invent a PR verdict.

A revise verdict goes back to the worker within the **same three-round total**;
a new head requires another fresh judge. At the cap, record needs-input with a
specific decision needed, keep incomplete PRs draft, and continue other issues.
Do not automatically retry the same blocker each polling cycle.

## 5. Squash-merge gate and continuation

For a workflow-owned PR, coordinator squash-merges only when:

1. Worker completed and an independent final judge passed the **current head SHA**.
2. PR is open, ready, nonconflicting, and all blocking findings/threads addressed.
3. Query both check runs (all pages/latest attempts) and commit status contexts for
   that SHA, repository required checks/rules, and external review feedback.
   Every applicable checker must finish successfully; required checks must exist.
   Failure, pending, cancelled, timed out, action-required, unknown/missing required
   checks or unreadable policy blocks merging. Neutral/skipped counts only when
   explicitly acceptable under repository policy; no checks is not proof of success.
   Do not require an approval review unless enforced repository rules require one.
4. Reread head, PR title, base and check state immediately before merging. If the
   head or base changed since validation, invalidate affected judge/check evidence
   and rerun the gate. Record `merging` with the expected head and intended message.
   Submit a squash merge with an **expected head SHA** so GitHub rejects a moved
   head. Do not use a merge tool that cannot enforce this precondition.

### Exact squash commit message

Use the current PR title followed by ` (#NUMBER)` as the commit title, matching
GitHub's usual prefilled title. Set the optional commit description/body to the
**explicit empty string**. For example, PR #123 titled `Fix WAL recovery` becomes:

```text
Fix WAL recovery (#123)
```

For the GitHub merge API, pass `merge_method: "squash"`, `sha: REVIEWED_HEAD_SHA`,
`commit_title: "Fix WAL recovery (#123)"`, and `commit_message: ""`.
Connected tools may name `sha` as `expected_head_sha`; use their actual schema.
Never omit the empty body or pass null, since that can select a default containing
commit messages. Do not copy the PR description, list individual commits, append
agent attribution, or add `Co-authored-by` trailers to the squash message. If a
tool drops empty strings or cannot set the title/body explicitly, use an API/CLI
that preserves them; otherwise checkpoint `merge-blocked` rather than accepting
an unwanted default message.

### Verify and continue

Verify GitHub reports the PR merged, capture the resulting commit SHA, and read
that commit to confirm its subject and empty body. Record `merged` with PR URL,
reviewed head, merge SHA and message verification, then continue to the next issue.
If GitHub unexpectedly changes the message, report the discrepancy as needs-input;
do not rewrite published default-branch history automatically.

After a timeout or ambiguous merge response, reread PR state before retrying:
if already merged, verify the merge commit and checkpoint it without another
merge request. If still open, rerun the gate; an unreadable result is blocked.
On insufficient permissions, disabled squash merging, enforced unmet review rules,
conflicts or merge rejection, record `merge-blocked` with the actual reason and
continue other issues. Do not switch accounts or change repository settings.
Same-author PRs do not need self-approval: the coordinator merges directly when
GitHub permits it and this gate passes.

On restart, treat legacy `approved`/`approval-blocked` checkpoints as open PRs
needing this merge gate, not as proof that the PR is merged or blocked forever.
Existing unmerged PRs continue to exclude their issues. Poll pending checks with
bounded waits, continuing other eligible work. End when the queue is empty, all
remaining work is blocked, user stops, or budget is low; do not busy-loop forever.
Before ending, persist all active states and report PR links, merged/blocked
status, claims needing handoff, and `$issue-loop REQUESTER_LOGIN` to resume.
