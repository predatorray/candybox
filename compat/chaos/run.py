#!/usr/bin/env python3
"""Opt-in, disposable, single-host real-process failure experiments (stdlib only)."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import random
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent


class InvariantViolation(Exception):
    pass


class HarnessFailure(Exception):
    pass


def command(args, *, timeout=120, env=None):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        raise HarnessFailure(f"{args!r}: exit {result.returncode}: {result.stderr[-4000:]}")
    return result.stdout


def validate_marker(namespace):
    if os.environ.get("CANDYBOX_CHAOS_DISPOSABLE") != "yes":
        raise HarnessFailure("Set CANDYBOX_CHAOS_DISPOSABLE=yes to authorize a fresh disposable cluster")
    if not re.fullmatch(r"chaos-[a-z0-9-]{1,40}", namespace):
        raise HarnessFailure("--namespace must match chaos-[a-z0-9-]{1,40}")


class Experiment:
    def __init__(self, args):
        validate_marker(args.namespace)
        self.args = args
        self.project = args.namespace + "-" + uuid.uuid4().hex[:12]
        self.output = Path(args.output).resolve() / self.project
        self.output.mkdir(parents=True, exist_ok=False)
        self.history = (self.output / "history.jsonl").open("a", buffering=1)
        self.env = dict(os.environ, CHAOS_NAMESPACE=self.project,
                        CHAOS_CANDYBOX_IMAGE=args.image, CHAOS_BALANCER_INTERVAL_MILLIS="0")
        self.base = ["docker", "compose", "-p", self.project, "-f", str(HERE / "compose.yml")]
        self.expected = {}
        self.recoveries = []
        self.started = False
        self.endpoint = None
        self.random = random.Random(args.seed)
        self.sequence = 0
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def event(self, kind, **fields):
        self.history.write(json.dumps(dict(event=kind, wall_ns=time.time_ns(),
                                           monotonic_ns=time.monotonic_ns(), **fields)) + "\n")
        self.history.flush()
        os.fsync(self.history.fileno())

    def compose(self, *args, timeout=120):
        return command(self.base + list(args), timeout=timeout, env=self.env)

    def targets(self, *services):
        # Validate every target immediately before destructive controls, including restoration.
        targets = []
        for service in services:
            ids = self.compose("ps", "-a", "-q", service).split()
            if len(ids) != 1:
                raise HarnessFailure(f"expected one disposable target for {service}: {ids}")
            info = json.loads(command(["docker", "inspect", ids[0]]))[0]
            labels = info["Config"]["Labels"]
            if (labels.get("com.docker.compose.project") != self.project or
                    labels.get("io.candybox.chaos.disposable") != self.project):
                raise HarnessFailure(f"refusing non-disposable target {service}")
            targets.append(ids[0])
        return targets

    def fault(self, action, *services):
        targets = self.targets(*services)
        self.event("fault-invoke", action=action, services=services)
        # Compose start may rerun exited dependencies, including one-shot metadata init.
        # Faults/restoration must touch only the exact existing, validated containers.
        command(["docker", *action.split(), *targets])
        self.event("fault-response", action=action, services=services)

    def request(self, method, key="", data=None, version=None, *, timeout=15):
        self.sequence += 1
        op = str(self.sequence)
        digest = hashlib.sha256(data).hexdigest() if data is not None else None
        self.event("invoke", operation_id=op, method=method, key=key, version=version, sha256=digest, timeout_seconds=timeout)
        headers = {"x-amz-meta-version": version} if version else {}
        request = urllib.request.Request(self.endpoint + "/chaos-data" + ("/" + key if key else ""),
                                         data=data, method=method, headers=headers)
        try:
            try:
                with self.http.open(request, timeout=timeout) as response:
                    body = response.read()
                    status, response_headers = response.status, dict(response.headers)
            except urllib.error.HTTPError as error:
                try:
                    body, status, response_headers = error.read(), error.code, dict(error.headers)
                finally:
                    error.close()
        except (OSError, urllib.error.URLError, TimeoutError, http.client.HTTPException) as error:
            self.event("response", operation_id=op, outcome="unknown", error=str(error))
            return None, b"", {}
        self.event("response", operation_id=op, outcome="response", status=status,
                   sha256=hashlib.sha256(body).hexdigest(), headers=response_headers)
        return status, body, {k.lower(): v for k, v in response_headers.items()}

    def put(self, key):
        version = uuid.UUID(int=self.random.getrandbits(128)).hex
        body = self.random.randbytes(8192)
        status, _, _ = self.request("PUT", key, body, version)
        if status and 200 <= status < 300:
            self.expected[key] = (hashlib.sha256(body).hexdigest(), version)
        return status

    def check_once(self, deadline=None):
        for key, (digest, version) in self.expected.items():
            if deadline is not None and time.monotonic() >= deadline:
                return False
            status, body, headers = self.request("GET", key)
            if status != 200:
                return False
            if hashlib.sha256(body).hexdigest() != digest or headers.get("x-amz-meta-version") != version:
                raise InvariantViolation(f"acknowledged object {key} has wrong bytes or metadata")
        return True

    def stabilize(self, scenario):
        start = time.monotonic()
        deadline = start + self.args.recovery_seconds
        while time.monotonic() < deadline:
            if self.check_once(deadline):
                duration = time.monotonic() - start
                self.recoveries.append(dict(scenario=scenario, observation_seconds=duration))
                self.event("stabilized", scenario=scenario, observation_seconds=duration)
                return
            time.sleep(1)
        raise InvariantViolation(f"acknowledged objects unavailable after {self.args.recovery_seconds}s: {scenario}")

    def setup(self):
        if command(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}"]).strip():
            raise HarnessFailure("fresh project unexpectedly exists")
        # Resolve Candybox before creating containers; tags cannot silently pull another build.
        info = json.loads(command(["docker", "image", "inspect", self.args.image]))[0]
        self.env["CHAOS_CANDYBOX_IMAGE"] = info["Id"]
        (self.output / "image.json").write_text(json.dumps(info, indent=2) + "\n")
        self.event("configuration", seed=self.args.seed, project=self.project, image=info["Id"],
                   source_revision=command(["git", "-C", str(HERE), "rev-parse", "HEAD"]).strip(),
                   source_dirty=bool(command(["git", "-C", str(HERE), "status", "--porcelain"]).strip()),
                   image_labels=info["Config"].get("Labels"),
                   experiment_timeout_seconds=self.args.experiment_seconds,
                   setup_timeout_seconds=self.args.setup_seconds,
                   recovery_timeout_seconds=self.args.recovery_seconds, objects=self.args.objects)
        configuration = self.compose("config")
        (self.output / "compose.yml").write_text(configuration)
        (self.output / "compose-bootstrap.yml").write_text(configuration)
        self.started = True  # partial setup also needs teardown
        self.compose("up", "-d", "--wait", "--wait-timeout", "300", timeout=360)
        (self.output / "containers-bootstrap.json").write_text(command(
            ["docker", "inspect", *self.compose("ps", "-a", "-q").split()]))
        address = self.compose("port", "gateway", "9711").strip()
        if not re.fullmatch(r"127\.0\.0\.1:[0-9]+", address):
            raise HarnessFailure(f"unexpected gateway binding {address!r}")
        self.endpoint = "http://" + address
        # Wait only for read-only gateway availability. A mutating timeout is never retried.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                with self.http.open(self.endpoint, timeout=2):
                    break
            except urllib.error.HTTPError:
                break  # HTTP error still proves the listener is serving
            except (OSError, urllib.error.URLError):
                time.sleep(1)
        status, _, _ = self.request("PUT", timeout=self.args.setup_seconds)
        if status not in (200, 201, 204):
            raise HarnessFailure(f"bucket creation failed: {status}")
        # A new descriptor is visible before every partition is initialized. Isolate that
        # known create/balance race from these acknowledged-data schedules; no PUT data yet.
        old_targets = self.targets("owner1", "owner2")
        (self.output / "bootstrap.log").write_text(self.compose("logs", "--no-color", "--timestamps", timeout=60))
        self.env["CHAOS_BALANCER_INTERVAL_MILLIS"] = "1000"
        (self.output / "compose.yml").write_text(self.compose("config"))
        self.event("bootstrap-transition-invoke", balancer_interval_millis=1000,
                   replaced_containers=old_targets, acknowledged_objects=len(self.expected))
        self.compose("up", "-d", "--no-deps", "--force-recreate", "--wait", "--wait-timeout", "120",
                     "owner1", "owner2", timeout=180)
        new_targets = self.targets("owner1", "owner2")
        if set(old_targets) & set(new_targets):
            raise HarnessFailure("bootstrap owner replacement did not recreate both containers")
        containers = json.loads(command(["docker", "inspect", *self.compose("ps", "-a", "-q").split()]))
        (self.output / "containers.json").write_text(json.dumps(containers, indent=2) + "\n")
        for info in containers:
            if info["Id"] in new_targets and "CANDYBOX_BALANCER_INTERVAL_MILLIS=1000" not in info["Config"]["Env"]:
                raise HarnessFailure("fault-phase owner is not configured for normal balancing")
        self.event("bootstrap-transition-response", balancer_interval_millis=1000,
                   owner_containers=new_targets, acknowledged_objects=len(self.expected))
        # Read-only readiness checks avoid sending ambiguous baseline PUTs while leases recover.
        deadline = time.monotonic() + self.args.recovery_seconds
        while time.monotonic() < deadline:
            ready = True
            for index in range(self.args.objects):
                if time.monotonic() >= deadline:
                    ready = False
                    break
                status, _, _ = self.request("GET", f"key-{index:04d}")
                if status == 200:
                    raise HarnessFailure("fresh bootstrap unexpectedly contains object data")
                if status != 404:
                    ready = False
                    break
            if ready:
                break
            time.sleep(1)
        else:
            raise HarnessFailure("normal balancing did not make empty baseline keys readable")
        if not ready:
            raise HarnessFailure("normal balancing readiness deadline exceeded")
        for index in range(self.args.objects):
            if self.put(f"key-{index:04d}") not in (200, 201, 204):
                raise HarnessFailure("baseline PUT did not acknowledge; inspect history")
        self.stabilize("baseline")

    def run(self):
        # Kill both candidates separately; whichever owns each key is necessarily killed.
        for owner in ("owner1", "owner2"):
            self.fault("kill -s SIGKILL", owner)
            try:
                self.stabilize("kill-" + owner)
            finally:
                self.fault("start", owner)
            self.stabilize("restart-" + owner)
        self.fault("pause", "owner1")
        try:
            time.sleep(15)  # greater than the explicit ten-second lease TTL
            self.stabilize("owner1-paused-past-ttl")
            if self.put("written-during-pause") not in (200, 201, 204):
                raise HarnessFailure("survivor did not acknowledge a fresh write")
        finally:
            self.fault("unpause", "owner1")
        self.stabilize("owner1-resumed")
        self.fault("stop", "bookie2", "bookie3")
        try:
            # A timeout is ambiguous. Any ACK becomes a durable obligation in expected.
            self.put("quorum-loss-outcome")
        finally:
            self.fault("start", "bookie2", "bookie3")
        self.stabilize("bookkeeper-quorum-restored")

    def cleanup(self):
        errors = []
        if not self.started:
            return errors
        # Unpause even when interruption occurred between command completion and response recording.
        for service in ("owner1", "owner2"):
            try:
                ids = self.compose("ps", "-a", "-q", service).split()
                if ids:
                    info = json.loads(command(["docker", "inspect", ids[0]]))[0]
                    if info["State"]["Paused"]:
                        self.fault("unpause", service)
            except Exception as error:
                errors.append(str(error))
        try:
            ids = self.compose("ps", "-a", "-q").split()
            if ids:
                (self.output / "containers-at-cleanup.json").write_text(command(["docker", "inspect", *ids]))
        except Exception as error:
            errors.append(str(error))
        try:
            (self.output / "cluster.log").write_text(self.compose("logs", "--no-color", "--timestamps", timeout=60))
        except Exception as error:
            errors.append(str(error))
        try:
            self.compose("down", "--volumes", "--remove-orphans", "--timeout", "10", timeout=90)
            if command(["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={self.project}"]).strip():
                errors.append("containers remain after teardown")
        except Exception as error:
            errors.append(str(error))
        self.event("cleanup", errors=errors)
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--image", required=True, help="locally built image; resolved to immutable ID")
    parser.add_argument("--output", default="/tmp/candybox-chaos")
    parser.add_argument("--seed", type=int, default=167)
    parser.add_argument("--objects", type=int, choices=range(1, 101), default=16)
    parser.add_argument("--experiment-seconds", type=int, choices=range(60, 3601), default=1200,
                        help="absolute setup/workload/fault deadline, followed by bounded cleanup (POSIX)")
    parser.add_argument("--setup-seconds", type=int, choices=range(1, 181), default=120,
                        help="one bucket creation request; never retried on ambiguity")
    parser.add_argument("--recovery-seconds", type=int, choices=range(1, 181), default=90)
    args = parser.parse_args()
    experiment = None
    status, detail, code = "harness-error", "setup not started", 2
    # Convert terminal cancellation into normal unwinding and bounded teardown.
    def interrupted(signum, frame):
        raise HarnessFailure(f"interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(args.experiment_seconds)
    try:
        experiment = Experiment(args)
        print(experiment.output, flush=True)
        experiment.setup()
        experiment.run()
        status, detail, code = "pass-initial-scenarios", "Initial schedules only; see uncovered acceptance cases", 0
    except InvariantViolation as error:
        status, detail, code = "invariant-violation", str(error), 1
    except Exception as error:
        status, detail, code = "harness-error", str(error), 2
    finally:
        signal.alarm(0)  # cleanup gets its own bounded subprocess budgets
        if experiment:
            errors = experiment.cleanup()
            if errors:
                status, detail, code = "harness-error", detail + "; cleanup: " + repr(errors), 2
            report = dict(status=status, detail=detail, seed=args.seed, recovery=experiment.recoveries,
                          acknowledged_objects=len(experiment.expected), cleanup_errors=errors,
                          uncovered=["ownership-aware stale-writer rejection", "selected-path network faults", "delayed relay consumer with destructive GC",
                                     "rename crash boundaries", "lost-response explicit-token retry",
                                     "real released-artifact mixed-version activation (#157)", "metrics snapshots"])
            (experiment.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            experiment.history.close()
    print(f"{status}: {detail}", file=sys.stderr)
    return code


if __name__ == "__main__":
    sys.exit(main())
