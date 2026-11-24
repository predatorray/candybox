"""Service-free checks for target refusal and the acknowledged-data oracle."""
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import unittest
from threading import Thread

import run as harness


class HarnessTest(unittest.TestCase):
    def test_explicit_marker_and_namespace_required(self):
        old = os.environ.pop("CANDYBOX_CHAOS_DISPOSABLE", None)
        try:
            with self.assertRaises(harness.HarnessFailure):
                harness.validate_marker("chaos-test")
            os.environ["CANDYBOX_CHAOS_DISPOSABLE"] = "yes"
            for namespace in ("production", "chaos-", "chaos-../prod", "chaos-Upper"):
                with self.assertRaises(harness.HarnessFailure):
                    harness.validate_marker(namespace)
            harness.validate_marker("chaos-test")
        finally:
            if old is None:
                os.environ.pop("CANDYBOX_CHAOS_DISPOSABLE", None)
            else:
                os.environ["CANDYBOX_CHAOS_DISPOSABLE"] = old

    def test_foreign_or_unmarked_container_is_never_faulted(self):
        experiment = harness.Experiment.__new__(harness.Experiment)
        experiment.project = "chaos-test-unique"
        calls = []
        def compose(*args):
            calls.append(args)
            return "container-id\n"
        experiment.compose = compose
        original = harness.command
        try:
            for labels in ({}, {"com.docker.compose.project": experiment.project},
                           {"com.docker.compose.project": "production",
                            "io.candybox.chaos.disposable": experiment.project}):
                harness.command = lambda args: json.dumps([{"Config": {"Labels": labels}}])
                with self.assertRaises(harness.HarnessFailure):
                    experiment.fault("kill -s SIGKILL", "owner1")
            self.assertTrue(all(call[0] == "ps" for call in calls))
        finally:
            harness.command = original

    def test_restore_starts_only_validated_container_without_dependencies(self):
        experiment = harness.Experiment.__new__(harness.Experiment)
        experiment.project = "chaos-test-unique"
        experiment.compose = lambda *args: "owner-container-id\n"
        events, commands = [], []
        experiment.event = lambda kind, **fields: events.append(kind)
        original = harness.command
        def command(args):
            commands.append(args)
            if args[1] == "inspect":
                return json.dumps([{"Config": {"Labels": {
                    "com.docker.compose.project": experiment.project,
                    "io.candybox.chaos.disposable": experiment.project}}}])
            return "owner-container-id\n"
        try:
            harness.command = command
            experiment.fault("start", "owner1")
            self.assertEqual(["docker", "start", "owner-container-id"], commands[-1])
            self.assertEqual(["fault-invoke", "fault-response"], events)
        finally:
            harness.command = original

    def oracle(self, response):
        experiment = harness.Experiment.__new__(harness.Experiment)
        experiment.expected = {"key": (hashlib.sha256(b"acknowledged").hexdigest(), "v1")}
        experiment.request = lambda *args: response
        return experiment

    def test_unavailability_is_retryable_but_corruption_is_violation(self):
        for status in (None, 404, 503):
            self.assertFalse(self.oracle((status, b"", {})).check_once())
        self.assertTrue(self.oracle((200, b"acknowledged", {"x-amz-meta-version": "v1"})).check_once())
        for body, headers in ((b"wrong", {"x-amz-meta-version": "v1"}),
                              (b"acknowledged", {}),
                              (b"acknowledged", {"x-amz-meta-version": "v0"})):
            with self.assertRaises(harness.InvariantViolation):
                self.oracle((200, body, headers)).check_once()

    def test_truncated_success_and_error_bodies_record_unknown_response(self):
        for response_status in (200, 503):
            class TruncatedResponse(BaseHTTPRequestHandler):
                def do_GET(self):
                    self.send_response(response_status)
                    self.send_header("Content-Length", "100")
                    self.end_headers()
                    self.wfile.write(b"partial")

                def log_message(self, *args):
                    pass

            with HTTPServer(("127.0.0.1", 0), TruncatedResponse) as server:
                thread = Thread(target=server.handle_request, daemon=True)
                thread.start()
                experiment = harness.Experiment.__new__(harness.Experiment)
                experiment.endpoint = f"http://127.0.0.1:{server.server_port}"
                experiment.sequence = 0
                experiment.http = harness.urllib.request.build_opener(harness.urllib.request.ProxyHandler({}))
                events = []
                experiment.event = lambda kind, **fields: events.append((kind, fields))
                self.assertEqual((None, b"", {}), experiment.request("GET", "key"))
                self.assertEqual(["invoke", "response"], [kind for kind, _ in events])
                self.assertEqual("unknown", events[-1][1]["outcome"])
                self.assertEqual(events[0][1]["operation_id"], events[-1][1]["operation_id"])
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

    def test_only_acknowledged_puts_enter_durability_obligations(self):
        experiment = harness.Experiment.__new__(harness.Experiment)
        experiment.random = harness.random.Random(167)
        experiment.expected = {}
        for status in (None, 400, 503, 200):
            experiment.request = lambda *args: (status, b"", {})
            experiment.put(str(status))
        self.assertEqual({"200"}, set(experiment.expected))


if __name__ == "__main__":
    unittest.main()
