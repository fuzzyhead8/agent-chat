import tempfile
import unittest
from pathlib import Path

from agent_chat.core import Coordinator


class ContextAttentionTests(unittest.TestCase):
    """Clients without a bridge (the Pi wake extension) apply the bridge's wake rule from context:
    a message wakes when it has no batch_id or when it carries attention."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-attention-")
        self.db = Path(self.tmp.name) / "state.sqlite3"
        self.a = self.register("a")
        self.b = self.register("b")

    def tearDown(self):
        self.tmp.cleanup()

    def register(self, name):
        coord = Coordinator(self.db, name)
        try:
            return coord.register(name)["session"]
        finally:
            coord.close()

    def test_explicit_group_carries_attention_and_quiet_broadcast_does_not(self):
        sender = Coordinator(self.db, self.a)
        try:
            direct = sender.send(self.b, "direct")["id"]
            explicit = sender.send_many([self.b], "explicit")[0]["id"]
            quiet = sender.send_group_prepared(None, "quiet", [])[0]["id"]
        finally:
            sender.close()
        reader = Coordinator(self.db, self.b)
        try:
            messages = {m["id"]: m for m in reader.context()["messages"]}
        finally:
            reader.close()
        self.assertNotIn("batch_id", messages[direct])
        self.assertNotIn("attention", messages[direct])
        self.assertIn("batch_id", messages[explicit])
        self.assertIs(messages[explicit]["attention"], True)
        self.assertIn("batch_id", messages[quiet])
        self.assertNotIn("attention", messages[quiet])


if __name__ == "__main__":
    unittest.main()
