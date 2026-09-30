import os
import subprocess
import sys
import time
import unittest

from agent_chat.processes import receipt_pid_alive


class ReceiptPidTests(unittest.TestCase):
    """Release receipts list the PIDs an agent started; a PID blocks release only while
    a process that began before the attested closure still runs."""

    def test_running_process_started_before_closure_blocks(self):
        self.assertTrue(receipt_pid_alive(os.getpid(), time.time()))

    def test_process_started_after_closure_does_not_block(self):
        closed_at = time.time() - 60
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            self.assertFalse(receipt_pid_alive(child.pid, closed_at))
        finally:
            child.kill()
            child.wait()

    def test_exited_process_does_not_block(self):
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        child.wait()
        self.assertFalse(receipt_pid_alive(child.pid, time.time()))


if __name__ == "__main__":
    unittest.main()
