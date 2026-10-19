# Releasing

Candybox publishes its library artifacts to [Maven Central](https://central.sonatype.com) under the
`me.predatorray.candybox` group, and attaches the `candybox-dist` archives to a GitHub Release. Releases are cut from release branches
by two workflows that you start by hand:
[`Cut release branch`](.github/workflows/release-branch.yml) and
[`Release`](.github/workflows/release.yml).

What is published: `candybox-parent` (POM), `candybox-common`, `candybox-bookkeeper`,
`candybox-coordination`, `candybox-lsm`, `candybox-protocol`, `candybox-server`, `candybox-client`,
`candybox-s3-gateway`, `candybox-admin-api` and `candybox-web`, each with `-sources` and `-javadoc`
jars and GPG signatures. `candybox-bookkeeper` and `candybox-coordination` also ship their `-tests`
jars, which hold the SPI contract suites. `candybox-integration-tests` and `candybox-dist` are not
published to Central.

Releases on Central are permanent. A version can never be changed or deleted, only superseded.

## One-time setup

1. **Central Portal account and namespace.** Sign in at <https://central.sonatype.com>, open
   *Namespaces*, and add `me.predatorray`. The Portal shows a verification key; publish it as a DNS
   `TXT` record on `predatorray.me` and press *Verify*. Once verified, the record can be removed.
2. **Portal user token.** In the Portal, open *View Account* → *Generate User Token*. The token is a
   username/password pair; it is not the account login.
3. **GPG signing key.**

   ```bash
   gpg --quick-gen-key "Wenhao Ji <predator.ray@gmail.com>" ed25519 sign 3y
   gpg --list-secret-keys --keyid-format long          # note the key id
   gpg --keyserver keyserver.ubuntu.com --send-keys <KEYID>
   gpg --keyserver keys.openpgp.org --send-keys <KEYID>
   gpg --armor --export-secret-keys <KEYID> > private.asc   # for the GitHub secret; delete afterwards
   ```

   Central checks signatures against public keyservers, so the upload must happen before the first
   release.
4. **GitHub secrets.** Under *Settings* → *Secrets and variables* → *Actions*, add these repository
   secrets, or add them to an environment named `maven-central` so you can put a required reviewer
   in front of releases:

   | Secret                   | Value                                  |
   |--------------------------|----------------------------------------|
   | `MAVEN_CENTRAL_USERNAME` | Portal user-token username             |
   | `MAVEN_CENTRAL_PASSWORD` | Portal user-token password             |
   | `GPG_PRIVATE_KEY`        | contents of `private.asc`              |
   | `GPG_PASSPHRASE`         | the key's passphrase                   |

5. **Let the workflows open PRs.** Turn on *Settings* → *Actions* → *General* → *Allow GitHub
   Actions to create and approve pull requests*. The workflows never push to `main` or to a release
   branch; every version change goes through a PR.
6. **Protect the release branches.** Give `release/*` the same branch protection or ruleset as
   `main`, so that changes reach them only through reviewed PRs with green CI. CI runs for pushes
   and PRs to `release/**` as well as `main`. If the ruleset restricts branch *creation*, let
   GitHub Actions bypass that one rule, because the Cut release branch workflow creates the branch.

## Branching model

Each minor line has a long-lived release branch, `release/X.Y.x`, and the releases tagged on it are
`vX.Y.0`, `vX.Y.1` and so on. `main` moves on to the next minor version as soon as the branch is
cut.

Fixes go **upstream first**. A fix lands on `main` through a normal PR, and if the release line
needs it, it is cherry-picked onto `release/X.Y.x` through a second PR:

```bash
git fetch origin
git switch -c cherry-pick/<topic> origin/release/0.1.x
git cherry-pick -x <commit-on-main>      # -x records which main commit it came from
git push origin HEAD                     # then open a PR against release/0.1.x
```

Release branches are never merged back into `main`, because `main` already has every fix. The
only commits that exist just on a release branch are its version changes. That way any merge
method works on both branches, squash included. If a fix cannot apply to `main` because the code
has moved on, fix the release branch directly and redo the change on `main` in a separate PR.

```
                 cut
main            ──●──────●───────────────●──────────▶   (0.2.0-SNAPSHOT after the cut)
                  │    fix A           fix B
                  │      ┆ cherry-pick   ┆ cherry-pick
release/0.1.x     └──────●── v0.1.0 ─────●── v0.1.1 ──▶   (0.1.x versions only)
```

## Cutting a release branch

1. Make sure `main` is green and holds what the release line should start from.
2. Open *Actions* → *Cut release branch* → *Run workflow* on `main`. The inputs are optional:
   - **commit**: the `main` commit to branch from. Empty means the latest `main`.
   - **next-version**: the version `main` moves to. Empty means the next minor
     (`0.1.0-SNAPSHOT` → `0.2.0-SNAPSHOT`).
3. The workflow creates `release/0.1.x` at that commit, still at `0.1.0-SNAPSHOT`, and opens a
   *Prepare 0.2.0-SNAPSHOT* PR against `main`. Merge that PR.

The workflow refuses to cut a line that already has a branch. It also refuses to cut from a commit
whose version differs from `main`'s, which usually means the line was already cut.

## Releasing from a release branch

1. Cherry-pick whatever the release still needs, and make sure the release branch is green.
2. Open *Actions* → *Release* → *Run workflow* and pick the **release branch** (for example
   `release/0.1.x`) as the branch. The inputs are optional:
   - **version**: the release version. Empty means the branch version without `-SNAPSHOT`
     (`0.1.0-SNAPSHOT` → `0.1.0`).
   - **next-version**: the version the release branch moves to. Empty means the next patch
     (`0.1.0` → `0.1.1-SNAPSHOT`).

   Both must belong to the branch's line (`0.1.*` on `release/0.1.x`).
3. If the `maven-central` environment has a required reviewer, approve the run.

The workflow then:

1. sets the release version in every POM, commits `Release X.Y.Z` and tags it `vX.Y.Z`, all
   without pushing;
2. runs the full build with unit and integration tests, builds the dashboard, signs everything,
   uploads one bundle to the Portal and waits until Central reports it as published;
3. pushes the release commit to a `version-bump/release-X.Y.x-<next>` branch, pushes the tag, and
   creates a GitHub Release with the `candybox-dist` archives;
4. sets the next `-SNAPSHOT` version, commits `Prepare <next>`, pushes it to the same branch and
   opens a PR against the release branch.

Merge that PR to finish the release. Until it is merged, the release branch still carries the old
`-SNAPSHOT` version, and the next Release run stops on the existing tag.

For a patch release, cherry-pick the fixes onto the same release branch and run *Release* on it
again. Patch releases need no new release branch.

Nothing is pushed until step 2 succeeds, so a run that fails during the build or the upload leaves
the repository untouched and can simply be started again. Once step 2 has passed, the version is on
Central for good. If a later step fails, finish that step by hand rather than re-running the
workflow, because a re-run fails on the existing tag.

The artifacts reach <https://repo.maven.apache.org/maven2/me/predatorray/candybox/> within about 30
minutes; search indexes take a few hours longer.

## About the version PRs

- Pushes and PRs made with the workflow's token do not trigger other workflows, so CI does not
  start on these PRs by itself. If the target branch requires status checks, close and reopen the
  PR to start CI.
- Any merge method works, squash included. The Release workflow's PR branch holds two commits:
  `Release X.Y.Z`, which the tag points to, and `Prepare <next>`. A squash merge lands only their
  combined diff (`X.Y.Z-SNAPSHOT` → `<next>`) on the release branch, so the tagged commit is not an
  ancestor of the branch. It stays reachable through the tag, which marks exactly the published
  source, so the `version-bump/*` branch can be deleted after merging. One visible effect:
  `git describe` on the release branch does not find the tag. Merge with a merge commit, where
  allowed, if you want the tag in the branch's history.
- If opening a PR fails, the run fails with a message naming the pushed branch. Everything before
  that step, including the release, is already done; open the PR by hand.
- If a tag ruleset stops the workflow from pushing `vX.Y.Z`, the version is already on Central by
  then, and the `Release X.Y.Z` commit has been pushed to the `version-bump/*` branch. The run fails
  before it opens the PR. Tag that commit by hand, create the GitHub Release, add the
  `Prepare <next>` commit to the branch and open the PR.

## Releasing from a workstation

Releasing by hand follows the same model: work on a checkout of the release branch, never `main`.
You need the same credentials locally. Put the token in `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>PORTAL_TOKEN_USERNAME</username>
      <password>PORTAL_TOKEN_PASSWORD</password>
    </server>
  </servers>
</settings>
```

Then run:

```bash
# Dry run: builds, signs and zips the bundle into target/central-publishing/ without uploading.
mvn -Prelease,frontend -DskipUpload=true deploy

# Upload, but stop at "validated" so the deployment can be inspected and published (or dropped)
# from the Portal's Deployments page.
mvn -Prelease,frontend -DautoPublish=false -DwaitUntil=validated deploy

# Upload and publish.
mvn -Prelease,frontend deploy
```

`-Pfrontend` is required. Without it, the `release` profile refuses to package `candybox-web`,
because the published jar would not contain the dashboard. gpg asks for the key's passphrase
through its agent; in a non-interactive shell, export `MAVEN_GPG_PASSPHRASE` instead.
