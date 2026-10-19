# Releasing

Candybox publishes its library artifacts to [Maven Central](https://central.sonatype.com) under the
`me.predatorray.candybox` group, and attaches the `candybox-dist` archives to a GitHub Release. Both
happen in the [`Release`](.github/workflows/release.yml) workflow, which you start by hand.

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

## Cutting a release

1. Make sure `main` is green and holds what you want to ship.
2. Open *Actions* → *Release* → *Run workflow*, keep the branch on `main`, and fill in the inputs
   or leave them empty:
   - **version**: the release version. Empty means the current version without `-SNAPSHOT`
     (`0.1.0-SNAPSHOT` → `0.1.0`).
   - **next-version**: the version `main` moves to afterwards. Empty means the next patch
     (`0.1.0` → `0.1.1-SNAPSHOT`).
3. If the `maven-central` environment has a required reviewer, approve the run.

The workflow then:

1. sets the release version in every POM, commits `Release X.Y.Z` and tags it `vX.Y.Z` (locally);
2. runs the full build with unit and integration tests, builds the dashboard, signs everything,
   uploads one bundle to the Portal and waits until Central reports it as published;
3. pushes the release commit to a `release/prepare-<next>` branch, pushes the tag, and creates a
   GitHub Release with the `candybox-dist` archives;
4. sets the next `-SNAPSHOT` version, commits `Prepare X.Y.(Z+1)-SNAPSHOT`, pushes it to the same
   branch and opens a PR to `main`. The workflow never pushes to `main`.

Merge that PR to finish the release.

Nothing is pushed until step 2 succeeds, so a run that fails during the build or the upload leaves
the repository untouched and can simply be started again. Once step 2 has passed, the version is on
Central for good. If a later step fails, finish that step by hand rather than re-running the
workflow, because a re-run fails on the existing tag.

The artifacts reach <https://repo.maven.apache.org/maven2/me/predatorray/candybox/> within about 30
minutes; search indexes take a few hours longer.

About the version PR:

- Opening it needs *Settings* → *Actions* → *General* → *Allow GitHub Actions to create and approve
  pull requests*. Without that setting, the branch is still pushed and the run fails with a message
  asking you to open the PR by hand. The release itself is already done by then.
- Pushes and PRs made with the workflow's token do not trigger other workflows, so CI does not
  start on that PR by itself. If `main` requires status checks, close and reopen the PR to start CI.
- Any merge method works, squash included. The branch holds two commits: `Release X.Y.Z`, which
  the tag points to, and `Prepare <next>`. A squash merge lands only their combined diff
  (`X.Y.Z-SNAPSHOT` → `<next>`) on `main`, so the release commit is not an ancestor of `main`. It
  stays reachable through the tag, which marks exactly the published source, so the
  `release/prepare-*` branch can be deleted after merging. One visible effect: `git describe` on
  `main` does not find the tag.
- If a tag ruleset stops the workflow from pushing `vX.Y.Z`, the version is already on Central by
  then, and the `Release X.Y.Z` commit has been pushed to `release/prepare-<next>`. The run fails
  before it opens the PR. Tag that commit by hand, create the GitHub Release, add the
  `Prepare <next>` commit to the branch and open the PR.

## Releasing from a workstation

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
