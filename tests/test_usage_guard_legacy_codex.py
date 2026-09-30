import tempfile
import unittest
from pathlib import Path

from agent_chat.rpc import RpcError
from agent_chat.usage import UsageStore
from agent_chat.usage_guard import UsageGuard


class Codex153Rpc:
    """Codex 0.153 reads account/rateLimits/read without params and rejects any params object."""

    def __init__(self):
        self.calls = []

    def connect(self): return self
    def close(self): pass

    def request(self, method, params=None):
        self.calls.append((method, params))
        if method == 'account/rateLimits/read':
            if params is not None:
                raise RpcError(-32600, 'Invalid request: invalid type: map, expected unit')
            return {'rateLimits': {'secondary': {'usedPercent': 96, 'windowDurationMins': 10080, 'resetsAt': 500}}}
        raise AssertionError('Unexpected Codex operation: ' + method)


class LegacyCodexQuotaTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = UsageStore(Path(directory.name) / 'usage.sqlite3')
        self.now = 100
        self.guard = UsageGuard(self, 'host', clock=lambda: self.now)

    def call(self, path, payload):
        payload = dict(payload)
        op = payload.pop('op')
        return getattr(self.store, op)(**payload, now=self.now)

    def test_weekly_quota_is_reported_when_codex_rejects_params(self):
        self.guard.check(Codex153Rpc())
        host = self.store.status(now=self.now)['hosts'][0]
        self.assertEqual((host['remaining_percent'], host['error']), (4, None))


if __name__ == '__main__':
    unittest.main()
