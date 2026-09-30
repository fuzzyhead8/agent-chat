"""Exclusive whole-file lock: fcntl.flock on POSIX, msvcrt on Windows."""
from __future__ import annotations

import os
import time

if os.name == 'nt':
    import errno
    import msvcrt

    def _at_start(fd: int, operation: int) -> None:
        # msvcrt locks a byte range from the current offset, so work on byte 0
        # and restore the offset for the caller's reads and writes.
        position = os.lseek(fd, 0, os.SEEK_CUR)
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, operation, 1)
        finally:
            os.lseek(fd, position, os.SEEK_SET)

    def lock_exclusive(fd: int, blocking: bool = True) -> None:
        """Hold the lock on this handle; closing the handle releases it.

        Without blocking, raise BlockingIOError while another handle holds it.
        """
        while True:
            try:
                _at_start(fd, msvcrt.LK_NBLCK)
                return
            except OSError as error:
                if error.errno not in (errno.EACCES, errno.EDEADLOCK):
                    raise
                if not blocking:
                    raise BlockingIOError(error.errno, 'file is locked by another handle') from None
                time.sleep(0.05)

    def unlock(fd: int) -> None:
        _at_start(fd, msvcrt.LK_UNLCK)
else:
    import fcntl

    def lock_exclusive(fd: int, blocking: bool = True) -> None:
        """Hold the lock on this handle; closing the handle releases it.

        Without blocking, raise BlockingIOError while another handle holds it.
        """
        fcntl.flock(fd, fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)
