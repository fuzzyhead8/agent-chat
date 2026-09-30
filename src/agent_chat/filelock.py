"""Exclusive whole-file lock: fcntl.flock on POSIX, msvcrt on Windows."""
from __future__ import annotations

import os
import time

if os.name == 'nt':
    import msvcrt

    def lock_exclusive(fd: int) -> None:
        """Block until this handle holds the lock; closing the handle releases it."""
        # msvcrt locks a byte range from the current offset, so lock byte 0
        # and restore the offset for the caller's reads and writes.
        position = os.lseek(fd, 0, os.SEEK_CUR)
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            while True:
                try:
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    return
                except OSError:
                    time.sleep(0.05)
        finally:
            os.lseek(fd, position, os.SEEK_SET)
else:
    import fcntl

    def lock_exclusive(fd: int) -> None:
        """Block until this handle holds the lock; closing the handle releases it."""
        fcntl.flock(fd, fcntl.LOCK_EX)
