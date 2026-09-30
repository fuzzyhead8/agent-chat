"""Monitor weekly account allowance and interrupt work while the reserve is blocked."""
from __future__ import annotations

import math
import time

from .core import CoordError
from .rpc import RpcError, TransportError


WEEK_MINUTES = 7 * 24 * 60
POLL_SECONDS = 10
ERRORS = (CoordError, RpcError, OSError, ValueError, KeyError, TypeError)


def weekly_allowance(result):
    """Use actual weekly windows, wherever they appear in the quota response."""
    if not isinstance(result, dict):
        raise CoordError('Codex returned invalid usage data')
    buckets = result.get('rateLimitsByLimitId')
    buckets = list(buckets.values()) if isinstance(buckets, dict) and buckets else [result.get('rateLimits')]
    candidates = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        for key in ('primary', 'secondary'):
            window = bucket.get(key)
            if not isinstance(window, dict) or window.get('windowDurationMins') != WEEK_MINUTES:
                continue
            used, resets_at = window.get('usedPercent'), window.get('resetsAt')
            if isinstance(used, bool) or not isinstance(used, (int, float)) or not math.isfinite(used) or used < 0:
                raise CoordError('Codex returned an invalid weekly usage percentage')
            if resets_at is not None and (isinstance(resets_at, bool) or not isinstance(resets_at, int) or resets_at < 0):
                raise CoordError('Codex returned an invalid weekly reset time')
            candidates.append((max(0, 100 - used), resets_at))
    if not candidates:
        raise CoordError('Weekly allowance unavailable from Codex; ChatGPT weekly quota is required')
    return min(candidates, key=lambda item: (item[0], item[1] if item[1] is not None else math.inf))


def _read_rate_limits(rpc):
    """Codex 0.155+ accepts excludeResetCreditDetails; 0.153 rejects any params ("expected unit")."""
    try:
        return rpc.request('account/rateLimits/read', {'excludeResetCreditDetails': True})
    except RpcError as error:
        if 'expected unit' not in str(error):
            raise
        return rpc.request('account/rateLimits/read')


class UsageGuard:
    def __init__(self, client, host, *, clock=time.monotonic):
        self.client, self.host, self.clock = client, host, clock
        self.policy = None
        self.next_poll = 0
        self.cleaned = set()
        self.stopped = set()
        self.enforcement_error = None

    def call(self, op, **params):
        result = self.client.call('/api/usage/rpc', {'op': op, 'host_id': self.host, **params})
        if (not isinstance(result, dict) or type(result.get('enabled')) is not bool
                or type(result.get('blocked')) is not bool):
            raise CoordError('Chat server returned invalid usage policy')
        return result

    def check(self, rpc):
        """Return policy after checking quota and enforcing any global pause."""
        previous = self.policy
        try:
            policy = self.call('status')
        except ERRORS as error:
            if previous is None:
                # With no policy yet, withhold wakes without assuming the user
                # enabled interruption of existing work.
                return {'enabled': False, 'paused': False, 'blocked': True,
                        'reason': 'Cannot verify usage protection: ' + str(error)}
            policy = dict(previous, blocked=previous['enabled'],
                          reason='Cannot verify usage protection: ' + str(error))
            return self._enforce(rpc, policy, report=False)

        if (self.clock() >= self.next_poll
                or (policy['enabled'] and (previous is None or not previous['enabled']))):
            self.next_poll = self.clock() + POLL_SECONDS
            remaining, resets_at, error_text = None, None, None
            try:
                rpc.connect()
                remaining, resets_at = weekly_allowance(_read_rate_limits(rpc))
            except ERRORS as error:
                error_text = str(error)
                if isinstance(error, TransportError):
                    rpc.close()
            try:
                policy = self.call('report', remaining_percent=remaining, resets_at=resets_at,
                                   error=error_text, stopped_threads=len(self.stopped),
                                   enforcement_error=self.enforcement_error)
            except ERRORS as error:
                # A lost report must not permit local work below the threshold.
                policy = dict(policy, blocked=policy['enabled'],
                              reason='Cannot report usage safely: ' + str(error))
                return self._enforce(rpc, policy, report=False)
        return self._enforce(rpc, policy)

    def _enforce(self, rpc, policy, *, report=True):
        self.policy = policy
        if not policy['blocked']:
            self.cleaned.clear()
            self.stopped.clear()
            self.enforcement_error = None
            return policy
        errors = []
        try:
            rpc.connect()
            for thread_id in self._loaded(rpc):
                try:
                    thread = rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})['thread']
                    if thread['status']['type'] == 'active':
                        self.cleaned.discard(thread_id)
                        turns = rpc.request('thread/turns/list', {
                            'threadId': thread_id, 'limit': 1,
                            'itemsView': 'summary', 'sortDirection': 'desc',
                        })['data']
                        for turn in turns:
                            if turn['status'] == 'inProgress':
                                try:
                                    rpc.request('turn/interrupt', {'threadId': thread_id, 'turnId': turn['id']})
                                except RpcError:
                                    # The turn may have completed between read and interrupt.
                                    if rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})['thread']['status']['type'] == 'active':
                                        raise
                        current = rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})['thread']
                        if current['status']['type'] == 'active':
                            errors.append('Thread is still stopping: ' + thread_id)
                        else:
                            self.stopped.add(thread_id)
                    # Native children and idle threads may still own background tools.
                    if thread_id not in self.cleaned:
                        rpc.request('thread/backgroundTerminals/clean', {'threadId': thread_id})
                        self.cleaned.add(thread_id)
                except ERRORS as error:
                    errors.append(thread_id + ': ' + str(error))
                    if isinstance(error, TransportError):
                        rpc.close()
                        rpc.connect()
        except ERRORS as error:
            errors.append(str(error))
        self.enforcement_error = '; '.join(errors)[:2000] or None
        if report:
            try:
                # Enforcement progress must not make cached quota appear fresh.
                self.call('enforcement', stopped_threads=len(self.stopped),
                          enforcement_error=self.enforcement_error)
            except ERRORS as error:
                self.enforcement_error = self.enforcement_error or str(error)
        return dict(policy, enforcement_error=self.enforcement_error,
                    stopped_threads=len(self.stopped))

    @staticmethod
    def _loaded(rpc):
        cursor, seen = None, set()
        for _ in range(100):
            params = {'limit': 100}
            if cursor:
                params['cursor'] = cursor
            page = rpc.request('thread/loaded/list', params)
            for thread_id in page['data']:
                if not isinstance(thread_id, str) or not thread_id:
                    raise CoordError('Codex returned an invalid loaded thread ID')
                if thread_id not in seen:
                    seen.add(thread_id)
                    yield thread_id
            cursor = page.get('nextCursor')
            if not cursor:
                return
        raise CoordError('Loaded Codex threads exceed the usage guard scan limit')
