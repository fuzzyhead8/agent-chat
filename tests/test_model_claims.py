import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator
from agent_chat.web import snapshot


class ModelClaimTests(unittest.TestCase):
    """Claude Code and Pi agents have no Codex thread to read a model from, so they declare it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-models-")
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

    def test_latest_declared_model_appears_in_the_ui_snapshot(self):
        coord = Coordinator(self.db, self.a)
        try:
            coord.set_model("gpt-6.1-sol")
            coord.set_model("claude-opus-5-5", "high")
        finally:
            coord.close()
        sessions = {s["id"]: s for s in snapshot(self.db)["sessions"]}
        self.assertEqual((sessions[self.a]["model"], sessions[self.a]["reasoning_effort"]), ("claude-opus-5-5", "high"))
        self.assertIsNone(sessions[self.b].get("model"))

    def test_blank_model_is_rejected(self):
        coord = Coordinator(self.db, self.a)
        try:
            with self.assertRaises(CoordError):
                coord.set_model("  ")
        finally:
            coord.close()


if __name__ == "__main__":
    unittest.main()
