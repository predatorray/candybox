#!/usr/bin/env bash
# Sets the project version everywhere the build records it: every POM, and candybox-web's
# package.json and package-lock.json. The release workflows use it for every version change, so
# these files never drift apart. Usage: .github/scripts/set-version.sh <version>
set -euo pipefail

if [ $# -ne 1 ] || [ -z "$1" ]; then
  echo "usage: $0 <version>" >&2
  exit 2
fi
version="$1"
cd "$(dirname "$0")/../.."

mvn -q -B -ntp versions:set -DnewVersion="${version}" -DgenerateBackupPoms=false -DprocessAllModules
(cd candybox-web && npm version "${version}" --no-git-tag-version --allow-same-version --ignore-scripts > /dev/null)
