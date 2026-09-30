#!/usr/bin/env python3
"""Local live conversation viewer and operator messaging for agent-chat."""
from __future__ import annotations

import contextlib
from email.parser import BytesParser
from email.policy import default as email_policy
from .filelock import lock_exclusive
import http.server
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import threading
import time
import tempfile
from urllib.parse import parse_qs, quote, urlsplit

from agent_chat.core import CoordError, Coordinator, agent_labels
from .projects import Projects
from .bridge_state import runtime_snapshot
from .usage import UsageStore
from .measurement import MeasurementStore
from .session_models import SessionModels
from .measurement_pause import request_pauses

from .auth import authorized, validate_config, origin

HERE = Path(__file__).resolve().parent
STATIC = {
    '/': ('index.html', 'text/html; charset=utf-8'),
    '/markdown.js': ('markdown.js', 'application/javascript; charset=utf-8'),
    '/app.js': ('app.js', 'application/javascript; charset=utf-8'),
    '/style.css': ('style.css', 'text/css; charset=utf-8'),
}
MESSAGE_COLUMNS = 'seq,id,sender_session,recipient_session,body,created_at,acked_at'
UI_MESSAGE_LIMIT = 50
MAX_MULTIPART_BYTES = 4 * 10 * 1024 * 1024 + 64 * 1024
CSP = "default-src 'self'; connect-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data: blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"


@contextlib.contextmanager
def reader(db_path):
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f'Coordinator database does not exist: {path}')
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if not row or row['value'] != '1':
            raise RuntimeError('Unsupported coordinator schema')
        yield conn
    finally:
        conn.close()


def attachment_table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachments'").fetchone())


def reply_table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_replies'").fetchone())


def batch_table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_batches'").fetchone())


def message_rows(rows, agents, db):
    items = [dict(row, sender_agent=agents.get(row['sender_session']),
                  recipient_agent=agents.get(row['recipient_session']), attachments=[],
                  reply_to=None, reply_preview=None, batch_id=None, deliveries=[]) for row in rows]
    if not items:
        return items
    by_message = {item['id']: item for item in items}
    marks = ','.join('?' for _ in by_message)
    if attachment_table(db):
        for row in db.execute(f'SELECT id,message_id,name,mime,size FROM attachments WHERE message_id IN ({marks}) ORDER BY rowid', list(by_message)):
            by_message[row['message_id']]['attachments'].append({
                'id': row['id'], 'name': row['name'], 'mime': row['mime'], 'size': row['size'],
                'url': '/api/attachments/' + row['id'],
            })
    if reply_table(db):
        query = f'''SELECT r.message_id,r.reply_to,p.seq,p.sender_session,p.recipient_session,p.body
                     FROM message_replies r JOIN messages p ON p.id=r.reply_to
                     WHERE r.message_id IN ({marks})'''
        for row in db.execute(query, list(by_message)):
            by_message[row['message_id']].update(reply_to=row['reply_to'], reply_preview={
                'id': row['reply_to'], 'seq': row['seq'], 'sender_session': row['sender_session'],
                'recipient_session': row['recipient_session'],
                'sender_agent': agents.get(row['sender_session']), 'body': row['body'][:240],
            })
    if batch_table(db):
        memberships = db.execute(
            f'SELECT message_id,batch_id FROM message_batches WHERE message_id IN ({marks})', list(by_message)).fetchall()
        batch_ids = sorted({row['batch_id'] for row in memberships})
        if batch_ids:
            batch_marks = ','.join('?' for _ in batch_ids)
            attention = ('EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id)'
                         if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_attention'").fetchone()
                         else '0')
            deliveries = {batch_id: [] for batch_id in batch_ids}
            for row in db.execute(
                f'''SELECT b.batch_id,m.id,m.recipient_session,m.acked_at,{attention} AS wake_requested
                     FROM message_batches b JOIN messages m ON m.id=b.message_id
                     WHERE b.batch_id IN ({batch_marks}) ORDER BY m.seq''', batch_ids):
                deliveries[row['batch_id']].append({
                    'id': row['id'], 'recipient_session': row['recipient_session'],
                    'recipient_agent': agents.get(row['recipient_session']), 'acked_at': row['acked_at'],
                    'wake_requested': bool(row['wake_requested']),
                })
            for row in memberships:
                by_message[row['message_id']].update(batch_id=row['batch_id'], deliveries=deliveries[row['batch_id']])
    return items


def snapshot(db_path, limit=UI_MESSAGE_LIMIT, usage_store=None, session_models=None):
    """Read a consistent view without advancing inboxes or changing ownership."""
    with reader(db_path) as db:
        now = time.time()
        sessions = [dict(row) for row in db.execute(
            'SELECT id,agent,registered_at FROM sessions ORDER BY registered_at,id')]
        bindings = {}
        if session_models is not None and db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_bindings'").fetchone():
            bindings = {row['session_id']: dict(row) for row in db.execute(
                'SELECT session_id,thread_id,agent_path FROM bridge_bindings')}
        agents = agent_labels(db)
        total = db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        rows = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages ORDER BY seq DESC LIMIT ?',
                          (limit,)).fetchall()
        messages = message_rows(list(reversed(rows)), agents, db)
        resources = []
        columns = 'name,owner_session,reservation_id,deadline,stale,reason'
        for row in db.execute(f'SELECT {columns} FROM resources ORDER BY name'):
            state = 'free' if not row['owner_session'] else (
                'stale' if row['stale'] or row['deadline'] <= now else 'owned')
            queue = [dict(session=item['session'], agent=agents.get(item['session']), position=i + 1)
                     for i, item in enumerate(db.execute(
                         'SELECT session FROM resource_queue WHERE resource=? ORDER BY seq',
                         (row['name'],)))]
            resources.append(dict(resource=row['name'], owner_session=row['owner_session'],
                                  owner_agent=agents.get(row['owner_session']),
                                  reservation_id=row['reservation_id'], state=state,
                                  deadline=row['deadline'], reason=row['reason'], queue=queue))
        bridge = runtime_snapshot(db, now)
        data = dict(bridge=bridge, server_time=now, sessions=sessions, messages=messages,
                    resources=resources, total_messages=total,
                    history_truncated=total > len(messages))
    if usage_store is not None:
        data['usage'] = usage_store.status()
    if session_models is not None:
        metadata = session_models.read(
            (route['thread_id'] for route in bindings.values() if route['thread_id']),
            (route['agent_path'] for route in bindings.values() if route['agent_path']))
        for session in sessions:
            route = bindings.get(session['id'], {})
            observed = metadata.get(route.get('thread_id') or route.get('agent_path'), {})
            session.update(model=observed.get('model'), reasoning_effort=observed.get('reasoning_effort'))
    return data


def page(db_path, before=None, limit=UI_MESSAGE_LIMIT, after=None, operator=None,
         agent=None, to_me=False):
    def valid_cursor(value):
        return value is None or (type(value) is int and 1 <= value <= 2**63 - 1)
    if ((before is not None and after is not None) or type(limit) is not int
            or not 1 <= limit <= 200 or not valid_cursor(before) or not valid_cursor(after)
            or (agent is not None and (not isinstance(agent, str) or not agent or len(agent) > 256))
            or (agent is not None or to_me) and not operator):
        raise ValueError('History requires a positive cursor and a limit from 1 to 200')
    with reader(db_path) as db:
        agents = agent_labels(db)
        conditions = []
        args = []
        if agent is not None or to_me:
            if batch_table(db):
                conditions.append('NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)')
            if agent is not None:
                inbound = '(m.sender_session=? AND m.recipient_session=?)'
                args.extend((agent, operator))
                if to_me:
                    conditions.append(inbound)
                else:
                    conditions.append(f'({inbound} OR (m.sender_session=? AND m.recipient_session=?))')
                    args.extend((operator, agent))
            else:
                conditions.append('m.recipient_session=?')
                args.append(operator)
        if after is not None:
            conditions.append('m.seq>?')
            args.append(after)
            direction = 'ASC'
        else:
            if before is not None:
                conditions.append('m.seq<?')
                args.append(before)
            direction = 'DESC'
        where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
        rows = db.execute(
            f'SELECT {MESSAGE_COLUMNS} FROM messages m {where} ORDER BY m.seq {direction} LIMIT ?',
            (*args, limit + 1)).fetchall()
        if after is not None:
            messages = rows[:limit]
        else:
            messages = list(reversed(rows[:limit]))
        return dict(messages=message_rows(messages, agents, db), has_more=len(rows) > limit)


def operator_session(db_path):
    """Keep one web operator identity per database, independent of agent env vars."""
    session_file = Path(str(db_path) + '.web-session.json')
    fd = os.open(session_file, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, 'r+') as stream:
        lock_exclusive(stream.fileno())
        raw = stream.read(4096)
        stored = json.loads(raw) if raw else None
        if stored is not None and (not isinstance(stored, dict) or not isinstance(stored.get('id'), str) or not stored['id']):
            raise ValueError('Invalid saved web operator identity')
        sid = stored['id'] if stored else 'web_operator_' + secrets.token_urlsafe(24)
        coord = Coordinator(db_path, sid)
        try:
            info = coord.register('operator')
        finally:
            coord.close()
        if not stored:
            stream.seek(0)
            json.dump({'id': info['session']}, stream)
            stream.truncate()
            stream.flush()
            os.fsync(stream.fileno())
        return info['session']


class WebServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, db_path, web_root, api_token=None, public_url=None, sessions_root=None):
        self.db_path = str(Path(db_path).expanduser().resolve())
        self.web_root = Path(web_root).resolve()
        self.stop_event = threading.Event()
        self.csrf_token = secrets.token_urlsafe(32)
        self.api_token = api_token
        self.listen_host = address[0]
        self.address_family = socket.AF_INET6 if ':' in address[0] else socket.AF_INET
        self.public_url = validate_config(address[0], api_token, public_url)
        self.bridge_manager = None
        self.usage = UsageStore(self.db_path)
        self.measurements = None
        self.projects = Projects(self.db_path)
        self.project_contexts = {}
        self.project_lock = threading.RLock()
        super().__init__(address, handler)
        try:
            self.sender_session = operator_session(self.db_path)
            if api_token:
                from .bridge_lease import BridgeManager
                self.bridge_manager = BridgeManager(self.db_path, usage_store=self.usage)
            self.measurements = MeasurementStore(self.db_path, sessions_root=sessions_root, usage_store=self.usage,
                                                 pause_callback=self.pause_measured_agents)
            self.session_models = SessionModels(self.measurements.sessions_root)
        except BaseException:
            self.server_close()
            raise

    @property
    def origin(self):
        host = '[' + self.listen_host + ']' if ':' in self.listen_host else self.listen_host
        return self.public_url or origin(f'http://{host}:{self.server_port}')

    def pause_measured_agents(self, project_id, database, measurement_id, recipients):
        current_path, sender, _ = self.project_context(project_id)
        if Path(database).resolve() != Path(current_path).resolve():
            raise CoordError('measurement project database changed')
        request_pauses(current_path, sender, measurement_id, recipients)

    def listed_projects(self):
        active = self.measurements.active_projects()
        return [dict(project, measurement_running=project['id'] in active)
                for project in self.projects.list()]

    def measurement_models(self, result):
        """Annotate reports using their recorded threads, including retired agents."""
        agents = [agent for key in ('active', 'latest') if result.get(key)
                  for agent in result[key]['agents']]
        metadata = self.session_models.read(agent['thread_id'] for agent in agents)
        for agent in agents:
            observed = metadata.get(agent['thread_id'], {})
            agent.update(model=observed.get('model'), reasoning_effort=observed.get('reasoning_effort'))
        return result

    def project_context(self, project):
        item = self.projects.get(project)
        if item['id'] == 'default':
            return self.db_path, self.sender_session, self.bridge_manager
        with self.project_lock:
            if item['id'] not in self.project_contexts:
                path = str(self.projects.db_path(item['id']))
                if not Path(path).is_file(): raise CoordError('project database is missing; restore it before continuing')
                sender = operator_session(path)
                manager = None
                if self.api_token:
                    from .bridge_lease import BridgeManager
                    manager = BridgeManager(path, usage_store=self.usage)
                self.project_contexts[item['id']] = (path, sender, manager)
            return self.project_contexts[item['id']]

    def server_close(self):
        if self.measurements:
            self.measurements.close()
        for _, _, manager in self.project_contexts.values():
            if manager: manager.close()
        if self.bridge_manager:
            self.bridge_manager.close()
        super().server_close()

    def shutdown(self):
        self.stop_event.set()
        super().shutdown()


class Handler(http.server.BaseHTTPRequestHandler):
    server: WebServer
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, *args):
        pass

    def trusted(self):
        expected = self.server.origin
        hosts = self.headers.get_all('Host', [])
        try:
            return (len(hosts) == 1 and origin(urlsplit(expected).scheme + '://' + hosts[0]) == expected and
                    (self.headers.get('Origin') is None or origin(self.headers['Origin']) == expected))
        except ValueError:
            return False

    def authenticate(self, bearer_only=False):
        if authorized(self.headers.get('Authorization', ''), self.server.api_token, bearer_only):
            return True
        raw = b'{"error":"Authentication required"}'
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Bearer realm="Agent chat"' if bearer_only else 'Basic realm="Agent chat", charset="UTF-8"')
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        self.wfile.write(raw)
        return False

    def read_json(self, limit=65536):
        if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
            raise ValueError('one Content-Length header is required; chunked requests are unsupported')
        if self.headers.get('Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
            raise ValueError('Expected JSON content')
        size = int(self.headers['Content-Length'])
        if not 1 <= size <= limit:
            raise ValueError('request exceeds the allowed size')
        raw = self.rfile.read(size)
        if len(raw) != size:
            raise ValueError('incomplete request')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError('Expected a JSON object')
        return value

    def respond(self, code, raw, mime, *, download_name=None):
        self.send_response(code)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Security-Policy', "default-src 'none'; sandbox" if download_name is not None else CSP)
        if download_name is not None:
            self.send_header('Content-Disposition', "attachment; filename*=UTF-8''" + quote(download_name, safe=''))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def select_project(self):
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        values = query.get('project', [])
        headers = self.headers.get_all('X-Agent-Chat-Project', [])
        if len(values) > 1 or len(headers) > 1 or (values and headers and values[0] != headers[0]):
            raise CoordError('choose exactly one project')
        project = (values or headers or ['default'])[0]
        self.project = self.server.projects.get(project)
        self.db_path, self.sender_session, self.bridge_manager = self.server.project_context(project)

    def scoped(self, data):
        if not hasattr(self, 'project') or self.project['id'] == 'default': return data
        if isinstance(data, list): return [self.scoped(item) for item in data]
        if isinstance(data, dict):
            return {key: (value + '?project=' + self.project['id'] if key == 'url' and isinstance(value, str) and value.startswith('/api/attachments/') else self.scoped(value)) for key, value in data.items()}
        return data

    def project_snapshot(self):
        return dict(snapshot(self.db_path, usage_store=self.server.usage, session_models=self.server.session_models), project=self.server.projects.get(self.project['id']), projects=self.server.listed_projects())

    def json(self, code, data):
        self.respond(code, json.dumps(self.scoped(data), ensure_ascii=False, separators=(',', ':')).encode('utf-8'), 'application/json; charset=utf-8')

    def do_GET(self):
        if not self.trusted():
            self.json(403, {'error': 'Untrusted host or origin'})
            return
        url = urlsplit(self.path)
        if not self.authenticate():
            return
        try:
            if url.path.startswith('/api/') and url.path != '/api/usage':
                self.select_project()
            if url.path == '/api/projects':
                self.json(200, {'projects': self.server.listed_projects(), 'project': self.project})
            elif url.path == '/api/usage':
                self.json(200, self.server.usage.status())
            elif url.path == '/api/measurements':
                self.json(200, self.server.measurement_models(self.server.measurements.status(self.project['id'])))
            elif url.path == '/api/snapshot':
                self.json(200, self.project_snapshot())
            elif url.path == '/api/config':
                self.json(200, {'sender': {'id': self.sender_session, 'agent': 'operator'},
                                'csrf_token': self.server.csrf_token, 'project': self.project,
                                'projects': self.server.listed_projects()})
            elif url.path == '/api/messages':
                params = parse_qs(url.query, keep_blank_values=True)
                before_values = params.get('before', [])
                after_values = params.get('after', [])
                limit_values = params.get('limit', [str(UI_MESSAGE_LIMIT)])
                agent_values = params.get('agent', [])
                to_me_values = params.get('to_me', [])
                if (len(before_values) > 1 or len(after_values) > 1 or len(limit_values) != 1
                        or len(agent_values) > 1 or len(to_me_values) > 1
                        or to_me_values not in ([], ['1'])):
                    raise ValueError('History cursor and limit parameters must appear once')
                if before_values and after_values:
                    raise ValueError('Choose either a before or after history cursor')

                def positive_integer(values, name):
                    if not values:
                        return None
                    value = values[0]
                    if not value.isascii() or not value.isdigit() or int(value) < 1:
                        raise ValueError(f'{name} must be a positive integer')
                    return int(value)

                before = positive_integer(before_values, 'before')
                after = positive_integer(after_values, 'after')
                limit = positive_integer(limit_values, 'limit')
                self.json(200, page(self.db_path, before, limit, after,
                                    self.sender_session, agent_values[0] if agent_values else None,
                                    bool(to_me_values)))
            elif url.path.startswith('/api/messages/'):
                message_id = url.path.removeprefix('/api/messages/')
                if set(parse_qs(url.query)) - {'project'} or not message_id.startswith('message_') or '/' in message_id or not all(char.isalnum() or char in '_-' for char in message_id):
                    self.json(404, {'error': 'Not found'})
                    return
                with reader(self.db_path) as db:
                    agents = agent_labels(db)
                    row = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages WHERE id=?', (message_id,)).fetchone()
                    item = message_rows([row], agents, db)[0] if row else None
                if item is None:
                    self.json(404, {'error': 'Not found'})
                else:
                    self.json(200, item)
            elif url.path.startswith('/api/attachments/'):
                attachment_id = url.path.removeprefix('/api/attachments/')
                if set(parse_qs(url.query)) - {'project'} or not attachment_id.startswith('attachment_') or '/' in attachment_id:
                    self.json(404, {'error': 'Not found'})
                    return
                with reader(self.db_path) as db:
                    if not attachment_table(db):
                        self.json(404, {'error': 'Not found'})
                        return
                    item = db.execute('SELECT name,mime,content FROM attachments WHERE id=?', (attachment_id,)).fetchone()
                if item is None:
                    self.json(404, {'error': 'Not found'})
                    return
                self.respond(200, item['content'], item['mime'],
                             download_name=None if item['mime'] in {'image/png', 'image/jpeg', 'image/gif', 'image/webp'} else item['name'])
            elif url.path == '/api/events':
                self.events()
            else:
                spec = STATIC.get(url.path)
                if spec is None:
                    self.json(404, {'error': 'Not found'})
                    return
                path = (self.server.web_root / spec[0]).resolve()
                if path.parent != self.server.web_root or not path.is_file():
                    self.json(404, {'error': 'Not found'})
                    return
                self.respond(200, path.read_bytes(), spec[1])
        except (ValueError, sqlite3.Error, RuntimeError, OSError) as error:
            self.json(400, {'error': str(error)})

    def do_POST(self):
        if not self.trusted():
            self.json(403, {'error': 'Untrusted host or origin'})
            return
        path = urlsplit(self.path).path
        machine_api = path in ('/api/coord', '/api/bridge/rpc', '/api/projects/rpc', '/api/usage/rpc')
        if not self.authenticate(bearer_only=machine_api):
            return
        try:
            if path not in ('/api/usage', '/api/usage/rpc'):
                self.select_project()
        except (CoordError, ValueError, sqlite3.Error, OSError) as error:
            self.json(400, {'error': str(error)})
            return
        if machine_api:
            try:
                data = self.read_json(60 * 1024 * 1024 if path == '/api/coord' else 1024 * 1024)
                if path == '/api/usage/rpc':
                    self.json(200, self.usage_action(data, machine=True))
                    return
                if path == '/api/projects/rpc':
                    self.json(200, self.project_action(data))
                    return
                if path == '/api/coord':
                    from .remote_service import dispatch
                    coord = Coordinator(self.db_path)
                    coord.session = None
                    coord.context_size = lambda value: len(json.dumps(
                        self.scoped(value), ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
                    try:
                        result = dispatch(coord, data, bridge_manager=self.bridge_manager)
                    finally:
                        coord.close()
                else:
                    result = {'result': self.bridge_manager.dispatch(data)}
                self.json(200, result)
            except (CoordError, ValueError, TypeError, KeyError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path not in ('/api/messages', '/api/sessions/remove', '/api/projects', '/api/projects/rename', '/api/usage', '/api/measurements'):
            self.json(404, {'error': 'Not found'})
            return
        csrf = self.headers.get('X-Agent-Chat-CSRF', '')
        if not csrf.isascii() or not secrets.compare_digest(csrf, self.server.csrf_token):
            self.json(403, {'error': 'Messaging connection expired. Reconnect and try again.'})
            return
        if path == '/api/measurements':
            try:
                data = self.read_json()
                if (isinstance(data, dict) and {'op', 'duration_seconds'} <= set(data)
                        and set(data) <= {'op', 'duration_seconds', 'pause_at_end'} and data['op'] == 'start'):
                    result = self.server.measurements.start(self.project['id'], self.db_path, data['duration_seconds'],
                                                            pause_at_end=data.get('pause_at_end', False))
                elif isinstance(data, dict) and data == {'op': 'stop'}:
                    result = self.server.measurements.stop(self.project['id'])
                else:
                    raise CoordError('invalid measurement operation')
                self.json(200, self.server.measurement_models(result))
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/usage':
            try:
                self.json(200, self.usage_action(self.read_json(), machine=False))
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path in ('/api/projects', '/api/projects/rename'):
            try:
                data = self.read_json()
                result = self.project_action(dict(data, op='create' if path == '/api/projects' else 'rename'))
                self.json(200, result)
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/sessions/remove':
            try:
                data = self.read_json()
                if set(data) != {'id'} or not isinstance(data['id'], str) or not data['id']:
                    raise ValueError('Session id must be a nonempty string')
                coord = Coordinator(self.db_path, self.sender_session)
                try:
                    result = coord.remove_session(data['id'])
                finally:
                    coord.close()
                self.json(200, result)
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        content_type = self.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
        if content_type not in ('application/json', 'multipart/form-data'):
            self.json(415, {'error': 'Expected a JSON message'})
            return
        try:
            if content_type == 'multipart/form-data':
                if len(self.headers.get_all('Content-Length', [])) != 1 or self.headers.get_all('Transfer-Encoding', []):
                    raise ValueError('Multipart uploads require one Content-Length and no Transfer-Encoding')
            size = int(self.headers.get('Content-Length', '0'))
            if content_type == 'application/json':
                if not 1 <= size <= 65536:
                    self.json(413, {'error': 'Message must fit within 64 KiB'})
                    return
                data = json.loads(self.rfile.read(size))
                attachments = None
            else:
                if not 1 <= size <= MAX_MULTIPART_BYTES:
                    self.json(413, {'error': 'Images must fit within 40 MiB plus 64 KiB of metadata'})
                    return
                data, attachments = self.read_multipart_message(size)
            if not isinstance(data, dict) or 'body' not in data or set(data) - {'to', 'body', 'reply_to'}:
                raise ValueError('Write a message with optional recipients')
            recipient, body = data.get('to'), data['body']
            is_many = recipient is None or isinstance(recipient, list)
            if ((recipient is not None and not isinstance(recipient, (str, list))) or not isinstance(body, str) or
                    (content_type == 'application/json' and not body.strip()) or
                    (content_type == 'multipart/form-data' and not body.strip() and not attachments) or
                    (isinstance(recipient, list) and (not recipient or any(not isinstance(item, str) or not item for item in recipient)))):
                raise ValueError('Choose a recipient and write a nonempty message')
            reply_to = data.get('reply_to')
            if reply_to is not None and (not isinstance(reply_to, str) or not reply_to):
                raise ValueError('Reply parent is invalid')
            with reader(self.db_path) as db:
                if isinstance(recipient, str):
                    if not db.execute('SELECT 1 FROM sessions WHERE id=?', (recipient,)).fetchone():
                        raise ValueError('Recipient is not a registered agent session')
                elif isinstance(recipient, list) and any(not db.execute('SELECT 1 FROM sessions WHERE id=?', (item,)).fetchone() for item in recipient):
                    raise ValueError('Recipient is not a registered agent session')
            with tempfile.TemporaryDirectory(prefix='agent-chat-upload-') as directory:
                upload_paths = self.write_uploads(directory, attachments or [])
                coord = Coordinator(self.db_path, self.sender_session)
                try:
                    sent = (coord.send_group_prepared(recipient, body.strip(), coord._prepare_attachments(upload_paths), reply_to)
                            if is_many else coord.send(recipient, body.strip(), attachments=upload_paths, reply_to=reply_to))
                finally:
                    coord.close()
            self.json(200, {'messages': sent} if is_many else sent)
        except (CoordError, ValueError, sqlite3.Error, RuntimeError, OSError) as error:
            self.json(400, {'error': str(error)})

    def read_multipart_message(self, size):
        """Decode the deliberately small form shape accepted by browser uploads."""
        raw = self.rfile.read(size)
        if len(raw) != size:
            raise ValueError('Malformed multipart message')
        content_type = self.headers.get('Content-Type', '')
        try:
            header = content_type.encode('ascii', 'strict')
        except UnicodeEncodeError as error:
            raise ValueError('Malformed multipart message') from error
        message = BytesParser(policy=email_policy).parsebytes(b'Content-Type: ' + header + b'\r\n\r\n' + raw)
        if not message.is_multipart() or message.defects:
            raise ValueError('Malformed multipart message')
        data = None
        has_message = False
        images = []
        for part in message.iter_parts():
            if part.defects or part.get_content_disposition() != 'form-data':
                raise ValueError('Malformed multipart message')
            name = part.get_param('name', header='content-disposition')
            if name == 'message':
                if has_message or part.get_filename() is not None:
                    raise ValueError('Multipart message must contain one message field')
                has_message = True
                try:
                    payload = part.get_payload(decode=True)
                    if payload is None:
                        raise ValueError('Multipart message field must be UTF-8 JSON')
                    if len(payload) > 64 * 1024:
                        raise ValueError('Multipart message metadata must fit within 64 KiB')
                    data = json.loads(payload.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError('Multipart message field must be UTF-8 JSON') from error
            elif name == 'images':
                filename = part.get_filename()
                if not filename:
                    raise ValueError('Attachment filename is required')
                content = part.get_payload(decode=True)
                if content is None:
                    raise ValueError('Malformed attachment upload')
                images.append((self.sanitize_upload_name(filename), content))
            else:
                raise ValueError('Multipart message contains an unsupported field')
        if not has_message:
            raise ValueError('Multipart message must contain one message field')
        return data, images

    @staticmethod
    def sanitize_upload_name(name):
        return Coordinator._attachment_name(name)

    @staticmethod
    def write_uploads(directory, uploads):
        if len(uploads) > 4:
            raise CoordError('at most 4 attachments are allowed')
        if any(len(content) > 10 * 1024 * 1024 for _, content in uploads):
            raise CoordError('attachment exceeds 10 MiB')
        paths = []
        for index, (name, content) in enumerate(uploads):
            child = Path(directory) / str(index)
            child.mkdir()
            path = child / name
            path.write_bytes(content)
            paths.append(path)
        return paths

    def project_action(self, data):
        op = data.get('op')
        if op == 'list' and set(data) == {'op'}:
            return {'projects': self.server.listed_projects()}
        if op == 'create' and set(data) == {'op', 'name'}:
            item = self.server.projects.create(data['name'])
            self.server.project_context(item['id'])
            return {'project': item, 'projects': self.server.listed_projects()}
        if op == 'rename' and set(data) == {'op', 'id', 'name'}:
            return {'project': self.server.projects.rename(data['id'], data['name']), 'projects': self.server.listed_projects()}
        raise CoordError('invalid project operation')

    def usage_action(self, data, machine):
        if not isinstance(data, dict) or not isinstance(data.get('op'), str):
            raise CoordError('invalid usage operation')
        op = data['op']
        if machine:
            if op == 'status' and set(data) == {'op', 'host_id'}:
                return self.server.usage.status(data['host_id'])
            fields = {'op', 'host_id', 'remaining_percent', 'resets_at', 'error', 'stopped_threads', 'enforcement_error'}
            if op == 'report' and set(data) <= fields and {'op', 'host_id', 'remaining_percent', 'resets_at'} <= set(data):
                return self.server.usage.report(data['host_id'], data['remaining_percent'], data['resets_at'],
                                                data.get('error'), data.get('stopped_threads', 0), data.get('enforcement_error'))
            if op == 'enforcement' and set(data) == {'op', 'host_id', 'stopped_threads', 'enforcement_error'}:
                return self.server.usage.enforcement(data['host_id'], data['stopped_threads'], data['enforcement_error'])
        else:
            if op == 'configure' and set(data) == {'op', 'enabled', 'threshold_percent'}:
                return self.server.usage.configure(data['enabled'], data['threshold_percent'])
            if op == 'resume' and set(data) == {'op'}:
                return self.server.usage.resume()
        raise CoordError('invalid usage operation')

    def events(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.end_headers()
        prior = None
        try:
            while not self.server.stop_event.is_set():
                data = self.scoped(self.project_snapshot())
                comparable = dict(data)
                comparable.pop('server_time')
                if comparable != prior:
                    payload = 'event: snapshot\ndata: ' + json.dumps(data, separators=(',', ':')) + '\n\n'
                    prior = comparable
                else:
                    payload = ': heartbeat\n\n'
                self.wfile.write(payload.encode())
                self.wfile.flush()
                self.server.stop_event.wait(1)
        except (OSError, sqlite3.Error, RuntimeError):
            pass
        finally:
            self.close_connection = True


def create_server(db_path, port=8765, web_root=None, *, host='127.0.0.1', api_token=None, public_url=None, sessions_root=None):
    with reader(db_path):
        pass
    return WebServer((host, port), Handler, db_path, web_root or HERE / 'web', api_token, public_url, sessions_root)
