"""Exercise ordering/privacy and real Git compare-and-create races offline."""

import concurrent.futures
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'queue.py'
spec = importlib.util.spec_from_file_location('issue_queue', SCRIPT)
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)


def issue(number, label, author='requester', date='2026-01-01T00:00:00Z', **extra):
    return dict(number=number, labels=[{'name': label}], user={'login': author},
                state='open', created_at=date, **extra)


class RankingTests(unittest.TestCase):
    def test_author_state_and_pr_filter(self):
        items = [issue(1, 'bug', author='other'), issue(2, 'bug'),
                 issue(3, 'bug', pull_request={}), issue(4, 'bug')]
        items[-1]['state'] = 'closed'
        self.assertEqual([2], [i['number'] for i in queue.rank(items, 'REQUESTER')])

    def test_type_then_age_with_stable_tie_break(self):
        items = [issue(5, 'documentation'), issue(4, 'enhancement'),
                 issue(3, 'testing'), issue(2, 'bug', date='2026-01-02T00:00:00Z'),
                 issue(1, 'bug'), issue(6, 'bug')]
        self.assertEqual([1, 6, 2, 3, 4, 5],
                         [i['number'] for i in queue.rank(items, 'requester')])


class ClaimTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.remote = self.root / 'remote.git'
        self.local = self.root / 'local'
        self.run_git('init', '--bare', str(self.remote))
        self.run_git('init', str(self.local))
        self.run_git('-C', str(self.local), 'config', 'user.name', 'Test')
        self.run_git('-C', str(self.local), 'config', 'user.email', 'test@example.invalid')
        self.run_git('-C', str(self.local), 'commit', '--allow-empty', '-m', 'Base')
        self.base = self.run_git('-C', str(self.local), 'rev-parse', 'HEAD')
        self.old_cwd = Path.cwd()
        os.chdir(self.local)

    def tearDown(self):
        os.chdir(self.old_cwd)
        self.temp.cleanup()

    @staticmethod
    def run_git(*args):
        return subprocess.run(['git', *args], check=True, capture_output=True,
                              text=True).stdout.strip()

    def claim(self):
        try:
            return queue.claim(12, self.base, str(self.remote))
        except RuntimeError:
            return None

    def test_concurrent_claims_have_exactly_one_winner_and_never_overwrite(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.claim(), range(4)))
        winners = [result for result in results if result]
        self.assertEqual(1, len(winners))
        self.assertIsNone(self.claim())
        remote_sha = self.run_git('--git-dir', str(self.remote), 'rev-parse',
                                  'refs/heads/issue-claims/12')
        self.assertEqual(winners[0]['claim_sha'], remote_sha)
        self.assertEqual(self.base, self.run_git('rev-parse', 'HEAD'))

    def test_uncertain_success_recovers_only_own_token(self):
        original_git = queue.git

        def lose_response(*args, **kwargs):
            result = original_git(*args, **kwargs)
            if args[0] == 'push':
                raise subprocess.CalledProcessError(1, ['git', 'push'])
            return result

        with patch.object(queue, 'git', side_effect=lose_response):
            self.assertIsNotNone(self.claim())
        self.assertIsNone(self.claim())

    def test_unreachable_remote_fails_closed(self):
        with self.assertRaises(subprocess.CalledProcessError):
            queue.claim(12, self.base, str(self.root / 'missing.git'))


if __name__ == '__main__':
    unittest.main()
