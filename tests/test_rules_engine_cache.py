"""Tests for the value-mapping cache integration in rules_engine.

Covers:
- Cache hit on a value the hardcoded transform misses → target + hit tracked.
- Cache miss + transform miss → unresolved_enums populated.
- Cache does NOT override values already in allowed_values.
- run_vehicle() auto-loads mappings (smoke).
"""

from __future__ import annotations

import unittest
import pandas as pd

from src import rules_engine, value_mappings as vm
from src.schemas import SCHEMAS


def _vehicle_allowed() -> dict[str, set]:
    return {
        fs.name: set(fs.allowed_values)
        for fs in SCHEMAS["vehicle"]
        if fs.allowed_values
    }


def _minimal_rules() -> dict:
    """Tiny rules YAML-equivalent: one source, usage + motorisation fields only."""
    return {
        "fields": {
            "usage": {
                "rules": [
                    {
                        "source": "client_file",
                        "priority": 1,
                        "column": "usage_raw",
                        "transform": "passthrough",
                    },
                ],
            },
            "motorisation": {
                "rules": [
                    {
                        "source": "client_file",
                        "priority": 1,
                        "column": "moto_raw",
                        "transform": "passthrough",
                    },
                ],
            },
            "registrationPlate": {
                "rules": [
                    {
                        "source": "client_file",
                        "priority": 1,
                        "column": "plaque",
                        "transform": "passthrough",
                    },
                ],
            },
        },
    }


class TestCacheIntegration(unittest.TestCase):
    def setUp(self):
        # Build an isolated mappings dict so tests don't depend on seed content.
        self.mappings: dict = {}
        vm.upsert(self.mappings, "vehicle", "usage", "VP", "private",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        vm.upsert(self.mappings, "vehicle", "motorisation", "SP98", "gas",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        vm.upsert(self.mappings, "vehicle", "motorisation", "Hybride-essence", "hybrid",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)

    def _run(self, df: pd.DataFrame):
        return rules_engine.apply_rules(
            _minimal_rules(),
            {"client_file": df},
            schema_name="vehicle",
            value_mappings=self.mappings,
            allowed_values_by_field=_vehicle_allowed(),
        )

    def test_cache_hit_normalizes_value(self):
        df = pd.DataFrame([
            {"plaque": "AA-001-AA", "usage_raw": "VP", "moto_raw": "SP98"},
            {"plaque": "BB-002-BB", "usage_raw": "VP", "moto_raw": "Hybride-essence"},
        ])
        result = self._run(df)
        # Values end up canonical.
        self.assertEqual(set(result.df["usage"]), {"private"})
        self.assertEqual(set(result.df["motorisation"]), {"gas", "hybrid"})
        # Hits tracked.
        self.assertEqual(len(result.value_mapping_hits), 4)  # 2 plates × 2 fields
        self.assertEqual(result.unresolved_enums, {})

    def test_cache_miss_marks_unresolved(self):
        df = pd.DataFrame([
            {"plaque": "AA-001-AA", "usage_raw": "Licorne", "moto_raw": "Plutonium"},
        ])
        result = self._run(df)
        # Values stay raw (we don't silently drop them).
        self.assertEqual(result.df.iloc[0]["usage"], "Licorne")
        self.assertEqual(result.df.iloc[0]["motorisation"], "Plutonium")
        # Both are tracked for AI fallback.
        self.assertEqual(len(result.unresolved_enums), 2)
        keys = set(result.unresolved_enums.keys())
        self.assertIn(("AA001AA", "usage"), keys)
        self.assertIn(("AA001AA", "motorisation"), keys)
        for schema, raw in result.unresolved_enums.values():
            self.assertEqual(schema, "vehicle")

    def test_allowed_value_bypasses_cache(self):
        """If the raw value is already canonical, don't touch it."""
        df = pd.DataFrame([
            {"plaque": "AA-001-AA", "usage_raw": "private", "moto_raw": "diesel"},
        ])
        result = self._run(df)
        self.assertEqual(result.df.iloc[0]["usage"], "private")
        self.assertEqual(result.df.iloc[0]["motorisation"], "diesel")
        # No cache hits, no unresolved — fast path.
        self.assertEqual(result.value_mapping_hits, {})
        self.assertEqual(result.unresolved_enums, {})

    def test_run_vehicle_autoloads_mappings(self):
        """Smoke: run_vehicle wires in the seed mappings without being told."""
        # Minimal source that exercises the plate + usage path through the real
        # vehicle.yml. Values chosen to hit the seed cache (SP98 isn't covered
        # by map_siv_motorisation but IS in the cache seed).
        df = pd.DataFrame([
            {
                "plaque": "CC-003-CC",
                "usage_client": "VP",
                "moto_client": "SP98",
            },
        ])
        overrides = {
            ("client_file", "registrationPlate"): "plaque",
            ("client_file", "usage"): "usage_client",
            ("client_file", "motorisation"): "moto_client",
        }
        result = rules_engine.run_vehicle(
            {"client_file": df}, manual_column_overrides=overrides,
        )
        # Usage : VP → private via the seed cache
        self.assertEqual(result.df.iloc[0]["usage"], "private")
        # Motorisation : SP98 → gas via cache (hardcoded transform wouldn't know)
        self.assertEqual(result.df.iloc[0]["motorisation"], "gas")


if __name__ == "__main__":
    unittest.main()
