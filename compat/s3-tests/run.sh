#!/usr/bin/env bash
#
# Run the ceph/s3-tests S3-compatibility suite against a running Candybox S3 gateway.
#
# The gateway needs a live cluster behind it. The easy path is docker compose from the repo root:
#
#     docker compose up -d                 # cluster + s3-gateway (published on :9711)
#     compat/s3-tests/run.sh --calibrate   # first run: discover what passes -> allowlist.txt
#     compat/s3-tests/run.sh               # thereafter: regression-gate the allowlist
#
# Modes:
#   (default)     run only the tests in allowlist.txt (the compatibility regression gate)
#   --all         run the whole boto3 functional suite and report (expect many failures: the
#                 v1 gateway is anonymous, path-style, no versioning/multipart/ACL/Range/conditionals)
#   --calibrate   run --all, then rewrite allowlist.txt with exactly the tests that PASSED,
#                 stamping the suite commit + endpoint they were calibrated against
#   -k EXPR       passed through to pytest (handy for poking at a single test)
#   -h|--help     this help
#
# Environment overrides:
#   S3_ENDPOINT   gateway base URL                (default: http://127.0.0.1:9711)
#   S3TESTS_REF   ceph/s3-tests git ref to pin    (default: master; calibrate records the SHA)
#   S3TESTS_REPO  clone URL                        (default: https://github.com/ceph/s3-tests.git)
#   TEST_PATH     suite path under the checkout    (default: s3tests_boto3/functional/test_s3.py)
#   PYTEST_ARGS   extra args appended to pytest    (default: empty)
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="$HERE/.work"
SUITE="$WORK/s3-tests"
VENV="$WORK/venv"
CONF="$WORK/s3tests.conf"
ALLOWLIST="$HERE/allowlist.txt"

S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9711}"
S3TESTS_REF="${S3TESTS_REF:-master}"
S3TESTS_REPO="${S3TESTS_REPO:-https://github.com/ceph/s3-tests.git}"
TEST_PATH="${TEST_PATH:-s3tests_boto3/functional/test_s3.py}"
PYTEST_ARGS="${PYTEST_ARGS:-}"

MODE="allowlist"
EXTRA_PYTEST=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --all)        MODE="all"; shift ;;
    --calibrate)  MODE="calibrate"; shift ;;
    -k)           EXTRA_PYTEST+=("-k" "$2"); shift 2 ;;
    -h|--help)    sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)            EXTRA_PYTEST+=("$1"); shift ;;
  esac
done

log() { printf '\033[1;34m[s3-tests]\033[0m %s\n' "$*" >&2; }

# --- endpoint -> host/port (path-style requires an IP literal host) -----------------------------
ENDPOINT_NO_SCHEME="${S3_ENDPOINT#*://}"
HOST="${ENDPOINT_NO_SCHEME%%:*}"
PORT="${ENDPOINT_NO_SCHEME##*:}"
[[ "$PORT" == "$ENDPOINT_NO_SCHEME" ]] && PORT="80"
if [[ "$HOST" == "localhost" ]]; then
  log "WARNING: host 'localhost' makes boto3 attempt virtual-hosted addressing; using 127.0.0.1 instead."
  HOST="127.0.0.1"
fi

# --- fetch + pin the suite ----------------------------------------------------------------------
mkdir -p "$WORK"
if [[ ! -d "$SUITE/.git" ]]; then
  log "cloning $S3TESTS_REPO -> $SUITE"
  git clone --quiet "$S3TESTS_REPO" "$SUITE"
fi
log "checking out ref '$S3TESTS_REF'"
git -C "$SUITE" fetch --quiet origin
git -C "$SUITE" checkout --quiet "$S3TESTS_REF"
git -C "$SUITE" pull --quiet --ff-only origin "$S3TESTS_REF" 2>/dev/null || true
SUITE_SHA="$(git -C "$SUITE" rev-parse --short HEAD)"
log "suite at $SUITE_SHA"

# --- venv ---------------------------------------------------------------------------------------
if [[ ! -x "$VENV/bin/pytest" ]]; then
  log "creating venv + installing the suite (one-time)"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet -r "$SUITE/requirements.txt"
  "$VENV/bin/pip" install --quiet -e "$SUITE"
fi

# --- render config ------------------------------------------------------------------------------
sed -e "s/__HOST__/$HOST/g" -e "s/__PORT__/$PORT/g" "$HERE/s3tests.conf.in" > "$CONF"
log "endpoint http://$HOST:$PORT  (config: $CONF)"

export S3TEST_CONF="$CONF"
cd "$SUITE"

run_pytest() { "$VENV/bin/pytest" -p no:cacheprovider "$@"; }

# shellcheck disable=SC2206  # intentional word-splitting of the PYTEST_ARGS env string
read -r -a PYTEST_ARGS_ARR <<< "${PYTEST_ARGS}"

case "$MODE" in
  allowlist)
    if [[ ! -s "$ALLOWLIST" ]]; then
      log "allowlist.txt is empty -- run '$0 --calibrate' first."; exit 2
    fi
    IDS=()
    while IFS= read -r line; do
      [[ -z "$line" || "$line" == \#* ]] && continue
      IDS+=("$line")
    done < "$ALLOWLIST"
    log "running ${#IDS[@]} allowlisted tests"
    run_pytest -v "${EXTRA_PYTEST[@]+"${EXTRA_PYTEST[@]}"}" "${IDS[@]}" \
      "${PYTEST_ARGS_ARR[@]+"${PYTEST_ARGS_ARR[@]}"}"
    ;;

  all)
    log "running the FULL boto3 functional suite (failures are expected for unsupported features)"
    run_pytest -v "${EXTRA_PYTEST[@]+"${EXTRA_PYTEST[@]}"}" "$TEST_PATH" \
      "${PYTEST_ARGS_ARR[@]+"${PYTEST_ARGS_ARR[@]}"}" || true
    ;;

  calibrate)
    log "calibrating: running the full suite and recording the tests that PASS"
    REPORT="$WORK/calibrate.out"
    run_pytest -v "${EXTRA_PYTEST[@]+"${EXTRA_PYTEST[@]}"}" "$TEST_PATH" \
      "${PYTEST_ARGS_ARR[@]+"${PYTEST_ARGS_ARR[@]}"}" | tee "$REPORT" || true
    {
      echo "# Calibrated allowlist for the Candybox S3 gateway -- ceph/s3-tests."
      echo "#"
      echo "# These are the test node IDs that PASSED against the gateway. Regenerate with:"
      echo "#     compat/s3-tests/run.sh --calibrate"
      echo "# and run the gate with:"
      echo "#     compat/s3-tests/run.sh"
      echo "#"
      echo "# suite ref : $S3TESTS_REF ($SUITE_SHA)"
      echo "# endpoint  : http://$HOST:$PORT"
      echo "# calibrated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
      echo "#"
      grep -E ' PASSED' "$REPORT" | awk '{print $1}' | sort -u
    } > "$ALLOWLIST"
    PASSES="$(grep -vcE '^\s*(#|$)' "$ALLOWLIST" || true)"
    log "wrote $PASSES passing tests to $ALLOWLIST"
    ;;
esac
