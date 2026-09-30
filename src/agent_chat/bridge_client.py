"""Authenticated HTTP bridge state with Codex execution kept on this machine."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

from .bridge import Bridge
from .filelock import lock_exclusive, unlock
from .core import CoordError
from .remote import HttpClient, host_id
from .rpc import RpcClient, RpcError, TransportError
from .usage_guard import UsageGuard


class RemoteBridgeState:
    def __init__(self, client, server_url, identity):
        self.client, self.server_url, self.identity = client, server_url, identity

    def call(self, op, **params):
        return self.client.call('/api/bridge/rpc', dict(self.identity, op=op, params=params))['result']

    def pending(self): return self.call('pending')
    def jobs(self): return self.call('jobs')
    def job(self, job_id): return self.call('job', job_id=job_id)
    def resolve(self, session_id): return self.call('resolve', session_id=session_id)
    def still_unread(self, job_id): return self.call('still_unread', job_id=job_id)
    def wake_messages(self, thread_id, message_ids):
        return self.call('wake-messages', thread_id=thread_id, message_ids=message_ids)
    def connection_metadata(self): return {'server': self.server_url, 'project': self.client.project}
    def prepare(self, thread_id, messages, payload):
        return self.call('prepare', thread_id=thread_id, messages=messages, payload=payload)
    def update(self, job_id, status, queue_id=None, error=None):
        return self.call('update', job_id=job_id, status=status, queue_id=queue_id, error=error)
    def observe(self, thread_id, state, error=None):
        return self.call('observe', thread_id=thread_id, state=state, error=error)
    def heartbeat(self, server, pid, error=None):
        return self.call('heartbeat', server=server, pid=pid, error=error)


@contextlib.contextmanager
def client_identity(server_url, state_file=None):
    directory = Path(os.environ.get('AGENT_CHAT_STATE_DIR', '~/.local/state/agent-chat')).expanduser()
    path = Path(state_file).expanduser() if state_file else directory / ('bridge-' + hashlib.sha256(server_url.encode()).hexdigest()[:24] + '.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    with Path(str(path) + '.lock').open('a+') as lock:
        try:
            lock_exclusive(lock.fileno(), blocking=False)
        except BlockingIOError:
            raise CoordError('a bridge client already uses this identity file')
        try:
            identity = {'owner': 'worker_' + uuid.uuid4().hex, 'secret': secrets.token_urlsafe(32), 'host_id': host_id()}
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                identity = json.loads(path.read_text())
            else:
                with os.fdopen(fd, 'w') as stream:
                    json.dump(identity, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
            if (not isinstance(identity, dict) or set(identity) != {'owner', 'secret', 'host_id'} or
                    not all(isinstance(v, str) and v for v in identity.values()) or identity['host_id'] != host_id()):
                raise CoordError('bridge identity file is invalid or belongs to another host')
            yield identity
        finally:
            unlock(lock.fileno())


def _project_ids(client):
    projects = client.call('/api/projects/rpc', {'op': 'list'}).get('projects')
    if not isinstance(projects, list):
        raise CoordError('remote project list is invalid')
    ids = []
    for project in projects:
        project_id = project.get('id') if isinstance(project, dict) else None
        if not isinstance(project_id, str) or not project_id:
            raise CoordError('remote project list is invalid')
        ids.append(project_id)
    return ids


def _identity_key(server_url, project):
    return server_url.rstrip('/') + ('#project=' + project if project != 'default' else '')


def _stop_codex(process):
    if process is None:
        return
    if os.name == 'nt':
        # Windows has no signalable process groups: taskkill /T ends the
        # app-server together with the npm shim and node processes around it.
        if process.poll() is None:
            subprocess.run(['taskkill', '/T', '/F', '/PID', str(process.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(5)
        return

    def group_alive():
        # Reap the leader before checking descendants, including after an
        # unexpected app-server exit. A zombie leader still occupies its group.
        process.poll()
        try:
            os.killpg(process.pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    for sig in (signal.SIGTERM, signal.SIGKILL):
        if not group_alive():
            break
        try:
            os.killpg(process.pid, sig)
        except (ProcessLookupError, PermissionError):
            # macOS may report EPERM while a signalled group disappears.
            # Continue checking rather than treating this as proof of closure.
            pass
        deadline = time.monotonic() + 5
        while group_alive() and time.monotonic() < deadline:
            time.sleep(.05)
    if group_alive():
        raise CoordError('could not prove owned Codex process group was closed')
    process.wait(timeout=1)


def _codex_launch(codex_bin):
    """Command prefix and Popen options for the owned app-server."""
    if os.name == 'nt':
        # npm installs codex as codex.cmd, which CreateProcess finds only by
        # its full name; a new group keeps the console Ctrl+C for the bridge.
        return [shutil.which(codex_bin) or codex_bin], {'creationflags': subprocess.CREATE_NEW_PROCESS_GROUP}
    return [codex_bin], {'start_new_session': True}


def _codex_environment(args, api_token):
    environment = os.environ.copy()
    environment['AGENT_CHAT_SERVER'] = args.server
    environment['AGENT_CHAT_API_TOKEN'] = api_token
    if args.project:
        environment['AGENT_CHAT_PROJECT'] = args.project
    else:
        environment.pop('AGENT_CHAT_PROJECT', None)
    for name in ('AGENT_CHAT_SESSION', 'AGENT_CHAT_TOKEN'):
        environment.pop(name, None)
    return environment


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, prog='agent-chat-client bridge')
    parser.add_argument('--server', default=os.environ.get('AGENT_CHAT_SERVER'))
    parser.add_argument('--api-token', '--token', dest='api_token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    parser.add_argument('--codex-server', default=os.environ.get('AGENT_CHAT_CODEX_SERVER', 'ws://127.0.0.1:4500'))
    parser.add_argument('--codex-bin', default='codex')
    parser.add_argument('--connect-only', action='store_true', help='attach to an existing local Codex app-server')
    parser.add_argument('--project', default=os.environ.get('AGENT_CHAT_PROJECT'))
    parser.add_argument('--state-file', help='private persistent worker identity; do not share between machines')
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--recover', action='store_true', help='release a lost dispatcher identity after confirming the old client stopped')
    parser.add_argument('--confirm-stopped', action='store_true')
    args = parser.parse_args(argv)
    if not args.server:
        parser.error('--server or AGENT_CHAT_SERVER is required')
    if not math.isfinite(args.interval) or args.interval < .2:
        parser.error('--interval must be finite and at least 0.2 seconds')
    if args.recover != args.confirm_stopped:
        parser.error('--recover requires --confirm-stopped, and vice versa')
    if args.state_file and not args.project and not args.recover:
        parser.error('--state-file requires --project when bridging all projects')
    previous, stop, stack = {}, threading.Event(), contextlib.ExitStack()
    rpc, codex = None, None
    records = {}
    usage_notice = None
    try:
        registry = HttpClient(args.server, args.api_token, project='default')
        if not isinstance(registry.token, str) or not registry.token.strip():
            raise CoordError('an API token is required to run the bridge client')
        if args.recover:
            project = args.project or 'default'
            client = HttpClient(args.server, args.api_token, project=project)
            with client_identity(_identity_key(args.server, project), args.state_file) as identity:
                RemoteBridgeState(client, args.server.rstrip('/'), identity).call('reset', confirm_stopped=True)
            print(json.dumps({'dispatcher': 'released', 'project': project}))
            return 0
        rpc = RpcClient(args.codex_server)
        usage_guard = UsageGuard(registry, host_id())
        if not args.connect_only:
            command, options = _codex_launch(args.codex_bin)
            codex = subprocess.Popen(
                command + ['app-server', '--listen', args.codex_server],
                cwd=os.getcwd(), env=_codex_environment(args, registry.token), stdout=sys.stderr, stderr=sys.stderr,
                **options)
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, lambda *_: stop.set())
        command = 'codex --remote ' + args.codex_server
        print(json.dumps({'bridge': 'starting', 'server': args.server, 'codex': args.codex_server,
                          'connect': 'Connect Codex with ' + command}), flush=True)

        def add_project(project):
            if project in records:
                return
            client = HttpClient(args.server, args.api_token, project=project)
            identity = stack.enter_context(client_identity(_identity_key(args.server, project), args.state_file))
            state = RemoteBridgeState(client, args.server.rstrip('/'), identity)
            records[project] = {'state': state, 'bridge': Bridge(state, rpc), 'acquired': False,
                                'last_error': None, 'ready': False}

        while not stop.is_set():
            if codex is not None and codex.poll() is not None:
                raise CoordError('local Codex app-server exited with status ' + str(codex.returncode))
            usage = usage_guard.check(rpc)
            if usage['blocked']:
                notice = (usage.get('reason'), usage.get('enforcement_error'))
                if notice != usage_notice:
                    print(json.dumps({'bridge': 'usage_paused', 'reason': notice[0],
                                      'error': notice[1]}), file=sys.stderr, flush=True)
                    usage_notice = notice
                for record in records.values():
                    record['ready'] = False
                    if record['acquired']:
                        try:
                            record['state'].heartbeat(args.codex_server, os.getpid(), usage.get('reason'))
                        except (CoordError, OSError, ValueError):
                            pass
                if args.once:
                    return 2
                stop.wait(args.interval)
                continue
            usage_notice = None
            try:
                projects = [args.project] if args.project else _project_ids(registry)
            except (CoordError, OSError, ValueError, KeyError, TypeError) as error:
                print(json.dumps({'bridge': 'waiting', 'error': str(error)}), file=sys.stderr, flush=True)
                if args.once:
                    return 2
                stop.wait(args.interval)
                continue
            for project in projects:
                try:
                    add_project(project)
                except (CoordError, OSError, ValueError) as error:
                    print(json.dumps({'bridge': 'waiting', 'project': project, 'error': str(error)}),
                          file=sys.stderr, flush=True)
            any_error = False
            for project in projects:
                if stop.is_set():
                    break
                if codex is not None and codex.poll() is not None:
                    raise CoordError('local Codex app-server exited with status ' + str(codex.returncode))
                record = records.get(project)
                if record is None:
                    any_error = True
                    continue
                state = record['state']
                try:
                    # Reacquiring the same identity is idempotent and confirms
                    # that a recovered server still recognizes this dispatcher.
                    state.call('acquire')
                    record['acquired'] = True
                    rpc.connect()
                    record['bridge'].tick()
                    state.heartbeat(args.codex_server, os.getpid())
                    if codex is not None and codex.poll() is not None:
                        raise CoordError('local Codex app-server exited with status ' + str(codex.returncode))
                    if not record['ready']:
                        print(json.dumps({'bridge': 'ready', 'project': project,
                                          'server': args.server, 'codex': args.codex_server}), flush=True)
                    record['ready'] = True
                    record['last_error'] = None
                except (CoordError, RpcError, TransportError, OSError, ValueError, KeyError, TypeError) as error:
                    rpc.close()
                    any_error = True
                    record['ready'] = False
                    text = str(error)
                    if text != record['last_error']:
                        print(json.dumps({'bridge': 'waiting', 'project': project, 'error': text}),
                              file=sys.stderr, flush=True)
                    record['last_error'] = text
                    if record['acquired']:
                        try:
                            state.heartbeat(args.codex_server, os.getpid(), text)
                        except (CoordError, OSError, ValueError):
                            pass
            if args.once:
                return 2 if any_error else 0
            stop.wait(args.interval)
    except (CoordError, OSError, ValueError) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        return 2
    finally:
        try:
            if rpc:
                rpc.close()
        except (OSError, ValueError):
            pass
        try:
            for project, record in records.items():
                if record['acquired']:
                    try:
                        record['state'].heartbeat(args.codex_server, 0, 'bridge stopped')
                        record['state'].call('release')
                    except (CoordError, OSError, ValueError) as error:
                        print(json.dumps({'bridge': 'lease_retained', 'project': project, 'error': str(error),
                                          'action': 'Restart with the same identity to reconcile; no automatic takeover.'}), file=sys.stderr)
        finally:
            try:
                stack.close()
            except Exception as error:
                print(json.dumps({'bridge': 'identity_cleanup_failed', 'error': str(error)}), file=sys.stderr)
            finally:
                try:
                    _stop_codex(codex)
                finally:
                    for sig, handler in previous.items():
                        signal.signal(sig, handler)
    return 0


if __name__ == '__main__':
    sys.exit(main())
