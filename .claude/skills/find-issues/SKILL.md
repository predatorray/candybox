---
name: find-issues
description: Review Candybox as an experienced distributed-systems architect and find bugs, scalability/SPOF risks, doc-vs-code drift and forgotten TODOs, then file de-duplicated GitHub issues. Use when asked to audit the project or find issues; optional argument narrows the focus area.
argument-hint: [focus area, e.g. "lsm", "docs", "s3-gateway"] [--file]
disable-model-invocation: true
---

# Find issues in Candybox

Act as an experienced software architect reviewing a distributed, scalable, S3-like object store. Focus: `$ARGUMENTS` (empty = all areas below). Review the HEAD of `main` (`git fetch origin main` first), and read history only when you need the "why".

Use the `mcp__github__*` tools for all GitHub work (load them via ToolSearch if needed); `gh` is not available in cloud sessions.

**Default is discuss-first:** post nothing to GitHub until the user has seen the findings. If `$ARGUMENTS` contains `--file`, skip the confirmation in step 4.

## 1. Orient
Read `README.md`, `CLAUDE.md`, `DESIGN.md` (especially §3 consistency, §7 fencing, §12 deliberate simplifications, §13 remaining work) and `OPERATIONS.md` ("Known limitations"). Skim `docs/content/docs/` and `compat/s3-tests/README.md` when relevant.

Do **not** report as issues the things the docs already list as deliberate v1 simplifications or known future work (fixed partition count, owner-only reads, no small-object inlining, buffered 16 MiB wire frames, no Syrup defragmentation, unsupported S3 features such as versioning/SSE/lifecycle/CORS). Report them only if the docs are wrong about them, or the code is worse than the doc claims.

## 2. Review, high level to low level
Pick areas from the list (or only `$ARGUMENTS`). Read the actual code, not just the docs.

1. **Architecture / scale.** Bottlenecks, scalability limits and single points of failure: ZooKeeper load and polling, partition-owner hot spots, the `PartitionBalancer` coordinator, scatter-gather listings, compaction and GC running on the owner, BookKeeper ledger counts, memory-buffered request bodies, gateway statelessness.
2. **Doc vs. code.** Compare `README.md`, `DESIGN.md`, `OPERATIONS.md`, `*_PLAN.md`, `docs/content/`, config defaults in `CandyboxConfig` and the config reference, CLI help, and S3 compat numbers against the code. Report each discrepancy with both sides quoted.
3. **Core-path correctness.** Fencing and handover (recover-open, never plain read-open), manifest CAS, HLC `observe` before stamping on recovery, WAL replay, flush and compaction commit, range tombstones, `MergingIterator`, GC reference counting and orphan handling, cross-partition rename-intent journal, multipart upload, SASL/TLS/ACL/SigV4 handling, record-format versioning and bounds-checked decoding.
4. **Unfinished work.** `grep -rn 'TODO\|FIXME\|XXX'` across `*.java`, `*.ts(x)`, scripts and docs. Judge each: tracked future work (already named in `DESIGN.md`) or genuinely forgotten?
5. **Tests and tooling.** Missing coverage on the paths above, contract suites (`LedgerStoreContract`, `CoordinationServiceContract`) that do not cover SPI behavior, CI/build gaps, Dockerfile/compose/K8s drift.

Only keep a finding when you can point at specific code or text (file and line) and explain a concrete failure scenario, not a vague smell. Mark each as **bug**, **improvement** or **docs**, and note your confidence. Verify a suspected bug by reading the callers, and by writing a quick failing test in the scratchpad when it is cheap. Do not commit that test.

## 3. De-duplicate against GitHub
Before presenting or filing anything, for every candidate:
- `search_issues` (open and closed) with 2-3 keyword variants: the symptom, the class/method name, and the feature.
- `list_pull_requests` / `search_pull_requests` for open PRs, and recently merged ones, that touch the same code or reference the same problem. Read the PR description and diff stat when a title looks close.
- Also check `git log origin/main` for a recent fix.

Then decide:
- **Already covered** by an open issue: drop it. If you have material new evidence, add a short comment on that issue instead of a new issue.
- **Being fixed** by an open PR: drop it; mention it to the user only if the PR looks incomplete.
- **Closed/fixed** already: drop it (re-open as a new issue only if it regressed, and link the old one).
- **Partially overlapping**: file it narrowly and link the related issue/PR in the body.

Never file two issues for the same root cause. Group symptoms under one issue.

## 4. Present, then file
Show the user a ranked list: title, type, severity, location, one-line why, and which duplicates you ruled out. Include a short architecture summary (bottlenecks/SPOFs) even when it produces no issue. Wait for the user's go-ahead, and let them drop or edit items (skipped when `--file` was passed).

To file each approved issue with `issue_write`:
- Title: short, specific, imperative or symptom-first. Prefix is not needed.
- Body, self-contained so another agent can pick it up without this conversation:
  - **Summary**: what is wrong, in two or three sentences.
  - **Where**: file paths and line numbers on `main` (note the commit SHA you reviewed).
  - **Scenario / repro**: concrete inputs or interleaving leading to the failure, or the doc text vs. code that disagree.
  - **Suggested fix**: options with a recommendation, kept brief.
  - **Related**: links to related issues/PRs found in step 3.
- Labels: use only labels that already exist in the repo (check with `get_label`), such as `bug`, `enhancement`, `documentation`. Do not create labels.
- Do not assign anyone.

End the issue body with the Claude Code attribution footer required for GitHub posts in cloud sessions.

## 5. Wrap up
Reply with the list of issues created (links), the ones you skipped as duplicates (with the issue/PR that covers them), and the areas you did not get to so a later run can pick them up. Do not fix anything in this run; fixing is the `resolve-issue` skill's job.

## Style
Be concise and factual. No praise, no speculation presented as fact, no restating what the code already says. Group nits; don't file one issue per typo.
