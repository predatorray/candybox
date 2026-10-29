#!/usr/bin/env python3
"""Offline requester-only ranking and atomic, non-expiring remote issue claims."""

import argparse
import json
import re
import subprocess
import sys
import uuid


def rank(issues, requester):
    def priority(issue):
        labels = " ".join(
            label if isinstance(label, str) else label["name"]
            for label in issue.get("labels", [])
        )
        words = set(re.findall(r"[a-z]+", labels.lower()))
        # Prefer explicit labels; use titles only for unlabeled issues.
        if not words:
            words = set(re.findall(r"[a-z]+", issue.get("title", "").lower()))
        groups = ({"bug", "bugs", "defect"}, {"test", "tests", "testing", "coverage"},
                  {"improvement", "enhancement", "feature"}, {"docs", "documentation"})
        category = next((i for i, group in enumerate(groups) if words & group), 2)
        return category, issue["created_at"], issue["number"]

    return sorted((issue for issue in issues
                   if issue.get("state") == "open" and "pull_request" not in issue
                   and issue.get("user", {}).get("login", "").casefold()
                   == requester.casefold()), key=priority)


def git(*args, input=None):
    return subprocess.run(["git", *args], input=input, text=True,
                          capture_output=True, check=True).stdout.strip()


def claim(number, base, remote):
    """Create only if absent; never steal an existing claim, even on retry."""
    if number < 1:
        raise ValueError("issue number must be positive")
    ref = f"refs/heads/issue-claims/{number}"
    parent = git("rev-parse", "--verify", f"{base}^{{commit}}")
    tree = git("rev-parse", f"{parent}^{{tree}}")
    message = f"Claim issue #{number}\n\nSession token: {uuid.uuid4()}\n"
    token = git("commit-tree", tree, "-p", parent, input=message)
    # An empty expected value requires the remote ref to be absent. This is an
    # atomic create, not permission to replace any existing claim.
    try:
        git("push", f"--force-with-lease={ref}:", remote, f"{token}:{ref}")
    except subprocess.CalledProcessError:
        # A network failure may follow a successful push. Only our exact unique
        # commit proves ownership; an unreadable remote must fail closed.
        observed = git("ls-remote", "--refs", remote, ref).split()
        if not observed or observed[0] != token:
            raise RuntimeError(f"Claim not acquired: {ref}; reconcile before retrying")
    return {"issue": number, "claim_ref": ref, "claim_sha": token}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    ranking = commands.add_parser("rank")
    ranking.add_argument("--requester", required=True)
    claiming = commands.add_parser("claim")
    claiming.add_argument("number", type=int)
    claiming.add_argument("--base", default="origin/main")
    claiming.add_argument("--remote", default="origin")
    args = parser.parse_args()
    try:
        if args.command == "rank":
            result = rank(json.load(sys.stdin)["issues"], args.requester)
        else:
            result = claim(args.number, args.base, args.remote)
        print(json.dumps(result, indent=2))
    except (KeyError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"queue: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
