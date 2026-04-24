"""Tests for src/session_reset.py.

Verifies that the "Nouvel import" action wipes import-scoped keys while
keeping auth flags, navigation, and cookie-manager internals intact.
"""

from __future__ import annotations

import unittest

from src.session_reset import reset_import_state, PRESERVED_KEYS


class TestResetImportState(unittest.TestCase):
    def _sample_state(self) -> dict:
        """Dict mimicking a realistic st.session_state after a full import."""
        return {
            # --- should be wiped ---
            "sources": {"file1": object()},
            "client_name": "ACME",
            "user_instructions": "some rules",
            "llm_proposals": {"file1": {"registrationPlate": "Plaque"}},
            "fleet_mapping": {"FR-BDX": "Bordeaux"},
            "step": 4,
            "engine_files": {"client.xlsx": {"df": None}},
            "engine_overrides": {("file1", "usage"): "Type"},
            "engine_result": object(),
            "value_mappings": {"usage": {}},
            "ai_fallback_report": object(),
            "engine_fleet_mapping": object(),
            "fleet_dialog_draft": {"column": "Agence"},
            "rules_overrides": {"vehicle": {"usage": ["source1"]}},
            # --- should survive ---
            "mode": "engine",
            "gh_check_result": {"ok": True},
            "rules_active_table": "vehicle",
            "_auth_ok": True,
            "_auth_checked": True,
            "rv_cookie_mgr": object(),
            "rv_cookie_set": "..",
        }

    def test_wipes_import_state(self):
        state = self._sample_state()
        reset_import_state(state)
        self.assertNotIn("sources", state)
        self.assertNotIn("client_name", state)
        self.assertNotIn("engine_files", state)
        self.assertNotIn("engine_result", state)
        self.assertNotIn("engine_fleet_mapping", state)
        self.assertNotIn("fleet_dialog_draft", state)
        self.assertNotIn("rules_overrides", state)
        self.assertNotIn("value_mappings", state)

    def test_preserves_auth_flags(self):
        state = self._sample_state()
        reset_import_state(state)
        self.assertTrue(state.get("_auth_ok"))
        self.assertTrue(state.get("_auth_checked"))

    def test_preserves_cookie_widget_state(self):
        """Keys prefixed rv_cookie_ must survive (extra-streamlit-components
        internal widget state — wiping it would unmount the component and
        kill the cookie)."""
        state = self._sample_state()
        reset_import_state(state)
        self.assertIn("rv_cookie_mgr", state)
        self.assertIn("rv_cookie_set", state)

    def test_preserves_nav(self):
        state = self._sample_state()
        reset_import_state(state)
        self.assertEqual(state.get("mode"), "engine")
        self.assertEqual(state.get("rules_active_table"), "vehicle")

    def test_returns_removed_keys(self):
        state = self._sample_state()
        removed = reset_import_state(state)
        self.assertIn("sources", removed)
        self.assertIn("engine_result", removed)
        self.assertNotIn("_auth_ok", removed)
        self.assertNotIn("mode", removed)

    def test_idempotent(self):
        """Calling reset twice on the same state is safe and a no-op the
        second time."""
        state = self._sample_state()
        reset_import_state(state)
        remaining_before = set(state.keys())
        removed2 = reset_import_state(state)
        self.assertEqual(removed2, [])
        self.assertEqual(set(state.keys()), remaining_before)

    def test_empty_state_is_noop(self):
        state: dict = {}
        removed = reset_import_state(state)
        self.assertEqual(removed, [])
        self.assertEqual(state, {})

    def test_unknown_keys_are_wiped(self):
        """Any unknown key (e.g. widget state like engine_edit_recognized_foo)
        should be wiped — this is the conservative behavior for the reset."""
        state = {
            "engine_edit_recognized_api_plaques": True,
            "engine_pending_pattern_client_file": {"pattern": "..."},
            "engine_snippet_foo": "bar",
            "_auth_ok": True,
        }
        reset_import_state(state)
        self.assertNotIn("engine_edit_recognized_api_plaques", state)
        self.assertNotIn("engine_pending_pattern_client_file", state)
        self.assertNotIn("engine_snippet_foo", state)
        self.assertTrue(state.get("_auth_ok"))

    def test_preserved_keys_includes_mode(self):
        """Sanity check on the whitelist contents."""
        self.assertIn("mode", PRESERVED_KEYS)
        self.assertIn("gh_check_result", PRESERVED_KEYS)


if __name__ == "__main__":
    unittest.main()
