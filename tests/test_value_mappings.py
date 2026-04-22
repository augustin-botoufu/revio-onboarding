"""Unit tests for src/value_mappings.py.

Covered:
- normalize_key: case, accents, punctuation, whitespace, empty input.
- load/dump roundtrip: YAML in, Python objects out, back to YAML.
- lookup: hit, miss, cross-schema isolation.
- upsert: create, update, status transitions, invalid input.
- validate_entry / delete_entry.
- iter_enum_fields against the real SCHEMAS.
"""

import io
import unittest
from datetime import date
from pathlib import Path

from src import value_mappings as vm
from src.schemas import SCHEMAS


class TestNormalizeKey(unittest.TestCase):
    def test_empty_inputs(self):
        self.assertEqual(vm.normalize_key(None), "")
        self.assertEqual(vm.normalize_key(""), "")
        self.assertEqual(vm.normalize_key("   "), "")

    def test_case_insensitive(self):
        self.assertEqual(vm.normalize_key("DIESEL"), "diesel")
        self.assertEqual(vm.normalize_key("dIeSeL"), "diesel")

    def test_strip_accents(self):
        self.assertEqual(vm.normalize_key("Électrique"), "electrique")
        self.assertEqual(vm.normalize_key("Hydrogène"), "hydrogene")
        self.assertEqual(vm.normalize_key("é"), "e")

    def test_collapse_punctuation(self):
        self.assertEqual(vm.normalize_key("Hybride-essence"), "hybride essence")
        self.assertEqual(vm.normalize_key("Hybride/Essence"), "hybride essence")
        self.assertEqual(vm.normalize_key("Hybride  essence"), "hybride essence")
        self.assertEqual(vm.normalize_key("HYBRIDE_ESSENCE"), "hybride essence")

    def test_same_key_regardless_of_form(self):
        """The crux of the whole matching system."""
        variants = [
            "Hybride-Essence",
            "hybride essence",
            "HYBRIDE/ESSENCE",
            "  Hybride/essence  ",
            "hybride---essence",
        ]
        keys = {vm.normalize_key(v) for v in variants}
        self.assertEqual(len(keys), 1)

    def test_preserves_digits(self):
        self.assertEqual(vm.normalize_key("SP98"), "sp98")
        self.assertEqual(vm.normalize_key("4 saisons"), "4 saisons")

    def test_strips_entirely_symbolic(self):
        """Pure symbols can't be indexed reliably — normalize to empty."""
        self.assertEqual(vm.normalize_key("---"), "")
        self.assertEqual(vm.normalize_key("///"), "")


class TestFieldKey(unittest.TestCase):
    def test_round_trip(self):
        fk = vm.field_key("vehicle", "motorisation")
        self.assertEqual(fk, "vehicle.motorisation")
        self.assertEqual(vm.parse_field_key(fk), ("vehicle", "motorisation"))

    def test_parse_missing_dot(self):
        self.assertEqual(vm.parse_field_key("nodot"), ("", "nodot"))


class TestUpsertLookup(unittest.TestCase):
    def test_insert_new(self):
        m: dict = {}
        entry = vm.upsert(
            m, "vehicle", "motorisation", "Diesel", "diesel",
            status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED,
        )
        self.assertEqual(entry.raw, "Diesel")
        self.assertEqual(entry.target, "diesel")
        self.assertEqual(entry.status, "validated")
        self.assertIsNotNone(entry.validated_at)

    def test_lookup_hit_case_insensitive(self):
        m: dict = {}
        vm.upsert(m, "vehicle", "motorisation", "Hybride-Essence", "hybrid",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        found = vm.lookup(m, "vehicle", "motorisation", "hybride essence")
        self.assertIsNotNone(found)
        self.assertEqual(found.target, "hybrid")

    def test_lookup_miss(self):
        m: dict = {}
        vm.upsert(m, "vehicle", "motorisation", "Diesel", "diesel",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        self.assertIsNone(vm.lookup(m, "vehicle", "motorisation", "Plutonium"))

    def test_cross_schema_isolation(self):
        """VP → private in vehicle.usage must not leak into driver.seniority."""
        m: dict = {}
        vm.upsert(m, "vehicle", "usage", "VP", "private",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        vm.upsert(m, "driver", "seniority", "VP", "leadership",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        self.assertEqual(
            vm.lookup(m, "vehicle", "usage", "VP").target, "private"
        )
        self.assertEqual(
            vm.lookup(m, "driver", "seniority", "VP").target, "leadership"
        )

    def test_upsert_updates_existing(self):
        m: dict = {}
        vm.upsert(m, "vehicle", "motorisation", "Hybride-Essence", "gas",
                  status=vm.STATUS_PENDING, source=vm.SOURCE_AI)
        # Second call with a different variant of the same raw → should
        # overwrite (same normalized key).
        vm.upsert(m, "vehicle", "motorisation", "hybride essence", "hybrid",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_MANUAL,
                  user="augustin@gorevio.co")
        found = vm.lookup(m, "vehicle", "motorisation", "Hybride-Essence")
        self.assertEqual(found.target, "hybrid")
        self.assertEqual(found.status, "validated")
        self.assertEqual(found.source, "manual")
        self.assertEqual(found.validated_by, "augustin@gorevio.co")

    def test_upsert_raises_on_empty_raw(self):
        m: dict = {}
        with self.assertRaises(ValueError):
            vm.upsert(m, "vehicle", "motorisation", "---", "diesel",
                      status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)

    def test_upsert_raises_on_empty_target(self):
        m: dict = {}
        with self.assertRaises(ValueError):
            vm.upsert(m, "vehicle", "motorisation", "Diesel", "",
                      status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)

    def test_pending_entry_has_no_validated_at(self):
        m: dict = {}
        entry = vm.upsert(m, "vehicle", "motorisation", "Hydrobus", "gas",
                          status=vm.STATUS_PENDING, source=vm.SOURCE_AI)
        self.assertIsNone(entry.validated_at)
        self.assertIsNone(entry.validated_by)


class TestValidateDelete(unittest.TestCase):
    def test_validate_pending(self):
        m: dict = {}
        vm.upsert(m, "vehicle", "motorisation", "Hydrogène bleu", "electric",
                  status=vm.STATUS_PENDING, source=vm.SOURCE_AI)
        updated = vm.validate_entry(
            m, "vehicle", "motorisation", "Hydrogène bleu",
            user="augustin@gorevio.co",
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated.status, "validated")
        self.assertEqual(updated.validated_by, "augustin@gorevio.co")

    def test_validate_missing(self):
        m: dict = {}
        self.assertIsNone(
            vm.validate_entry(m, "vehicle", "motorisation", "Absent")
        )

    def test_delete_existing(self):
        m: dict = {}
        vm.upsert(m, "vehicle", "motorisation", "Diesel", "diesel",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)
        self.assertTrue(vm.delete_entry(m, "vehicle", "motorisation", "Diesel"))
        self.assertNotIn("vehicle.motorisation", m)  # empty bucket pruned

    def test_delete_missing(self):
        m: dict = {}
        self.assertFalse(vm.delete_entry(m, "vehicle", "motorisation", "Nope"))


class TestIterEnumFields(unittest.TestCase):
    def test_finds_known_fields(self):
        fields = vm.iter_enum_fields(SCHEMAS)
        triples = {(s, f) for s, f, _ in fields}
        # Spot-check the ones Augustin cares about most:
        self.assertIn(("vehicle", "usage"), triples)
        self.assertIn(("vehicle", "motorisation"), triples)
        self.assertIn(("driver", "civility"), triples)
        self.assertIn(("contract", "isHT"), triples)
        self.assertIn(("contract", "tiresType"), triples)
        self.assertIn(("asset", "kind"), triples)

    def test_allowed_values_correct(self):
        fields = vm.iter_enum_fields(SCHEMAS)
        d = {(s, f): av for s, f, av in fields}
        self.assertEqual(
            set(d[("vehicle", "motorisation")]),
            {"diesel", "gas", "hybrid", "electric"},
        )


class TestRoundTrip(unittest.TestCase):
    def test_dump_then_load(self):
        m: dict = {}
        vm.upsert(m, "vehicle", "motorisation", "Hybride-Diesel", "hybrid",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED,
                  note="Véhicule hybride.")
        vm.upsert(m, "driver", "civility", "Mr", "1",
                  status=vm.STATUS_VALIDATED, source=vm.SOURCE_SEED)

        yaml_str = vm.dump_yaml(m)
        tmp = Path("/tmp/test_value_mappings_roundtrip.yml")
        tmp.write_text(yaml_str, encoding="utf-8")

        reloaded = vm.load(tmp)
        self.assertIn("vehicle.motorisation", reloaded)
        self.assertIn("driver.civility", reloaded)
        vm_entry = vm.lookup(reloaded, "vehicle", "motorisation", "hybride diesel")
        self.assertIsNotNone(vm_entry)
        self.assertEqual(vm_entry.target, "hybrid")
        self.assertEqual(vm_entry.note, "Véhicule hybride.")

    def test_load_missing_file(self):
        reloaded = vm.load(Path("/tmp/does_not_exist_xyzzy.yml"))
        self.assertEqual(reloaded, {})

    def test_load_broken_yaml_returns_empty(self):
        bad = Path("/tmp/broken_mappings.yml")
        bad.write_text("mappings: [not valid:", encoding="utf-8")
        self.assertEqual(vm.load(bad), {})


class TestRealSeed(unittest.TestCase):
    """Smoke test against the actually committed seed file."""

    def test_seed_has_expected_buckets(self):
        mappings = vm.load()  # default path
        if not mappings:
            self.skipTest("Seed not present in this checkout")
        self.assertIn("vehicle.motorisation", mappings)
        self.assertIn("vehicle.usage", mappings)

    def test_seed_hybride_diesel_is_hybrid(self):
        """The decision from Augustin 2026-04-22 must be preserved."""
        mappings = vm.load()
        if not mappings:
            self.skipTest("Seed not present in this checkout")
        for variant in ["Hybride-Diesel", "hybride diesel", "Hybride/Diesel"]:
            found = vm.lookup(mappings, "vehicle", "motorisation", variant)
            self.assertIsNotNone(found, f"Missing mapping for {variant!r}")
            self.assertEqual(
                found.target, "hybrid",
                f"Expected {variant!r} → hybrid, got {found.target}",
            )


if __name__ == "__main__":
    unittest.main()
