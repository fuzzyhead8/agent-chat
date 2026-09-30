"""Small, deliberately conservative HTTP transport for a remote coordinator."""
from __future__ import annotations

import json
import math
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from .core import CoordError
from .filelock import lock_exclusive

MAX_RESPONSE_BYTES = 60 * 1024 * 1024


class RemoteCoordError(CoordError):
    """The remote coordinator could not safely complete a request."""


def _loopback(host: str) -> bool:
    if host.lower() in ("localhost", "127.0.0.1", "::1"):
        return True
    # Do not DNS-resolve arbitrary names: a rebinding response could turn a
    # token-bearing HTTP request into a request to a different service.
    return False


def client_host_id() -> str:
    """Return a stable, locally persisted client host identity.

    Tests and ephemeral runners should set AGENT_CHAT_HOST_ID explicitly.
    """
    explicit = os.environ.get("AGENT_CHAT_HOST_ID")
    if explicit:
        return explicit
    root = Path(os.environ.get("AGENT_CHAT_STATE_DIR") or (Path.home() / ".local" / "state" / "agent-chat"))
    path = root / "client-host.json"
    root.mkdir(parents=True, exist_ok=True)
    # A lock plus fsync/replace prevents a concurrent client from reading a
    # partially written identity.  Existing malformed state fails closed.
    lock = root / "client-host.lock"
    with lock.open("a+", encoding="utf-8") as guard:
        os.chmod(lock, 0o600)
        lock_exclusive(guard.fileno())
        try:
            value = json.loads(path.read_text(encoding="utf-8")).get("host_id")
        except FileNotFoundError:
            import secrets
            value = "host_" + secrets.token_urlsafe(24)
            temp = root / (".client-host." + secrets.token_hex(8))
            with temp.open("x", encoding="utf-8") as stream:
                os.chmod(temp, 0o600)
                json.dump({"host_id": value}, stream)
                stream.flush(); os.fsync(stream.fileno())
            os.replace(temp, path)
        except (OSError, ValueError, AttributeError) as error:
            raise RemoteCoordError("client host identity file is invalid; set AGENT_CHAT_HOST_ID to recover") from error
        if not isinstance(value, str) or not value:
            raise RemoteCoordError("client host identity file is invalid; set AGENT_CHAT_HOST_ID to recover")
        return value


# Public short name used by adapters and tests.
host_id = client_host_id


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RemoteCoordError("remote coordinator redirect refused")


class HttpClient:
    def __init__(self, server_url: str, api_token: str | None = None, timeout: float = 10, project: str | None = None):
        parsed = urllib.parse.urlsplit(server_url)
        if parsed.scheme not in ("https", "http") or not parsed.hostname:
            raise RemoteCoordError("server URL must be an absolute HTTP(S) URL")
        if parsed.username is not None or parsed.password is not None:
            raise RemoteCoordError("server URL must not contain credentials")
        if parsed.scheme == "http" and not _loopback(parsed.hostname):
            raise RemoteCoordError("HTTP is allowed only for an explicit loopback SSH tunnel; use HTTPS")
        if parsed.query or parsed.fragment or parsed.path not in ('', '/'):
            raise RemoteCoordError("server URL must be an origin without a path, query or fragment")
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0 or timeout > 120:
            raise RemoteCoordError("timeout must be between 0 and 120 seconds")
        self.server_url = server_url.rstrip("/")
        self.token = api_token if api_token is not None else os.environ.get("AGENT_CHAT_API_TOKEN")
        self.timeout = float(timeout)
        self.project = project or os.environ.get("AGENT_CHAT_PROJECT") or "default"
        self._opener = urllib.request.build_opener(_NoRedirect)

    def call(self, path: str, payload: dict) -> dict:
        if path not in ("/api/coord", "/api/bridge/rpc", "/api/projects/rpc", "/api/usage/rpc"):
            raise RemoteCoordError("remote coordinator path is not permitted")
        if not isinstance(payload, dict):
            raise RemoteCoordError("remote coordinator payload must be an object")
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_RESPONSE_BYTES:
            raise RemoteCoordError("remote coordinator request is too large")
        headers = {"Content-Type": "application/json", "Accept": "application/json", "X-Agent-Chat-Project": self.project}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(self.server_url + path, data=encoded, headers=headers, method="POST")
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                data = response.read(MAX_RESPONSE_BYTES + 1)
        except RemoteCoordError:
            raise
        except urllib.error.HTTPError as error:
            with error:
                data = error.read(MAX_RESPONSE_BYTES + 1)
            try:
                message = json.loads(data.decode("utf-8")).get("error", error.reason)
            except (ValueError, UnicodeDecodeError, AttributeError):
                message = error.reason
            raise RemoteCoordError("remote coordinator rejected request: " + str(message)) from error
        except (urllib.error.URLError, OSError, TimeoutError) as error:
            raise RemoteCoordError("remote coordinator is unavailable: " + str(error)) from error
        if len(data) > MAX_RESPONSE_BYTES:
            raise RemoteCoordError("remote coordinator response is too large")
        try:
            result = json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as error:
            raise RemoteCoordError("remote coordinator returned invalid JSON") from error
        if not isinstance(result, dict):
            raise RemoteCoordError("remote coordinator returned an invalid response")
        # HTTP errors are handled above. Successful wake-job responses also
        # contain an "error" field (including recovery/audit information).
        return result
