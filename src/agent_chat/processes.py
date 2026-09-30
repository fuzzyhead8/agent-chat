"""Conservative, host-local checks for receipt process closure."""

from datetime import datetime, timezone
import os
import subprocess
import time


def receipt_pid_alive(pid: int, closed_at: float) -> bool:
    """A reused PID is harmless only if its current process began after closure.

    ps exposes start times on both macOS and Linux. Its whole-second timestamp
    is a lower bound: equality is ambiguous and must continue to block release.
    Missing/inaccessible process metadata also leaves the hold in place.
    """
    if (isinstance(closed_at, bool) or not isinstance(closed_at, (int, float))
            or not 0 < closed_at <= time.time()):
        raise ValueError("receipt closed_at is invalid")
    if os.name == 'nt':
        return _windows_pid_alive(pid, closed_at)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        result = subprocess.run(
            ['ps', '-p', str(pid), '-o', 'lstart='],
            env=dict(os.environ, LC_ALL='C', TZ='UTC0'),
            capture_output=True, text=True, timeout=5, check=True,
        )
        started = datetime.strptime(result.stdout.strip(), '%a %b %d %H:%M:%S %Y')
        return started.replace(tzinfo=timezone.utc).timestamp() <= closed_at
    except (OSError, ValueError, subprocess.SubprocessError):
        return True


def _windows_pid_alive(pid: int, closed_at: float) -> bool:
    """Windows has no signal 0: os.kill(pid, 0) sends CTRL_C_EVENT. Ask the process table instead."""
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    kernel32.GetProcessTimes.argtypes = (wintypes.HANDLE,) + (ctypes.POINTER(wintypes.FILETIME),) * 4
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        # ERROR_INVALID_PARAMETER means no such process; access denial keeps the hold.
        return ctypes.get_last_error() != 87
    try:
        code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return True
        if code.value != 259:  # not STILL_ACTIVE: exited, only a handle keeps the entry
            return False
        times = [wintypes.FILETIME() for _ in range(4)]
        if not kernel32.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)):
            return True
        ticks = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
        return ticks / 10_000_000 - 11_644_473_600 <= closed_at  # FILETIME epoch is 1601
    finally:
        kernel32.CloseHandle(handle)
