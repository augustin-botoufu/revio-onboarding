"""Tests for src/ai_normalization.py.

The LLM call is mocked: we don't hit the network, we just verify that given
a known LLM response, the module:
- upserts pending entries into the mappings dict,
- patches the df / orphan_df cells,
- records resolved counts + errors.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src import ai_normalization as ain
from src import value_mappings as vm


class TestRunAIFallback(unittest.TestCase):
    def test_happy_path(self):
        mappings: dict = {}
        df = pd.DataFrame(
            {"motorisation": ["Plutonium", "SP98", None]},
            index=["AB123CD", "EF456GH", "IJ789KL"],
        )
        unresolved = {
            ("AB123CD", "motorisation"): ("vehicle", "Plutonium"),
            ("EF456GH", "motorisation"): ("vehicle", "SP98"),
        }

        fake_response = {
            "mappings": {"Plutonium": None, "SP98": "gas"},
            "_notes": ["Plutonium n'est pas un carburant reconnu"],
            "_debug": {"batch_size": 2},
        }
        with patch("src.ai_normalization.llm_mapper.propose_enum_mappings",
                   return_value=fake_response):
            report = ain.run_ai_fallback(mappings, unresolved, df=df)

        # df was patched for SP98 → gas, Plutonium left as-is.
        self.assertEqual(df.at["EF456GH", "motorisation"], "gas")
        self.assertEqual(df.at["AB123CD", "motorisation"], "Plutonium")
        # Mappings dict has SP98 → gas with pending status.
        hit = vm.lookup(mappings, "vehicle", "motorisation", "SP98")
        self.assertIsNotNone(hit)
        self.assertEqual(hit.target, "gas")
        self.assertEqual(hit.status, vm.STATUS_PENDING)
        self.assertEqual(hit.source, vm.SOURCE_AI)
        # Plutonium was NOT added (mapped to null).
        self.assertIsNone(vm.lookup(mappings, "vehicle", "motorisation", "Plutonium"))
        # Report accurate.
        self.assertEqual(report.total_resolved, 1)
        self.assertEqual(report.total_proposed, 1)
        self.assertIn(("vehicle", "motorisation"), report.still_unresolved)
        self.assertEqual(report.errors, {})

    def test_llm_error_captured_not_thrown(self):
        mappings: dict = {}
        unresolved = {("AB123CD", "motorisation"): ("vehicle", "FooBar")}
        with patch("src.ai_normalization.llm_mapper.propose_enum_mappings",
                   return_value={"_error": "Boom", "_notes": []}):
            report = ain.run_ai_fallback(mappings, unresolved)
        self.assertIn(("vehicle", "motorisation"), report.errors)
        self.assertFalse(report.ok)
        self.assertEqual(report.total_resolved, 0)
        # Nothing was written to mappings.
        self.assertEqual(mappings, {})

    def test_rejects_targets_outside_allowed(self):
        """If the LLM invents a target not in allowed_values, we drop it."""
        mappings: dict = {}
        df = pd.DataFrame({"motorisation": ["MysteryFuel"]}, index=["X"])
        unresolved = {("X", "motorisation"): ("vehicle", "MysteryFuel")}
        with patch(
            "src.ai_normalization.llm_mapper.propose_enum_mappings",
            return_value={"mappings": {"MysteryFuel": "nuclear"}, "_notes": []},
        ):
            report = ain.run_ai_fallback(mappings, unresolved, df=df)
        # df unchanged, no upsert, cell counted as still_unresolved.
        self.assertEqual(df.at["X", "motorisation"], "MysteryFuel")
        self.assertEqual(mappings, {})
        self.assertEqual(report.total_resolved, 0)

    def test_patches_orphan_df(self):
        mappings: dict = {}
        df = pd.DataFrame({"motorisation": []})
        orphan_df = pd.DataFrame({"motorisation": ["SP98"]}, index=["GHOST"])
        unresolved = {("GHOST", "motorisation"): ("vehicle", "SP98")}
        with patch(
            "src.ai_normalization.llm_mapper.propose_enum_mappings",
            return_value={"mappings": {"SP98": "gas"}, "_notes": []},
        ):
            report = ain.run_ai_fallback(
                mappings, unresolved, df=df, orphan_df=orphan_df,
            )
        self.assertEqual(orphan_df.at["GHOST", "motorisation"], "gas")
        self.assertEqual(report.total_resolved, 1)


if __name__ == "__main__":
    unittest.main()
