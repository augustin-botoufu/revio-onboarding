"""Tests for src/fleet_segmentation.py.

Covers the pure logic: unique values listing, mapping build with merge,
plate index computation, split by fleet, slugify / sheet-name sanitization.
"""

from __future__ import annotations

import unittest

import pandas as pd

from src import fleet_segmentation as fs


class TestUniqueValues(unittest.TestCase):
    def test_counts_and_order(self):
        df = pd.DataFrame(
            {"Agence": ["Bdx", "Bdx", "Paris", "BORDEAUX", "Lyon"]},
        )
        got = fs.unique_values_in_column(df, "Agence")
        # Case preserved; count desc; ties alpha.
        # Bdx=2, Paris=1, BORDEAUX=1, Lyon=1
        self.assertEqual(got[0], ("Bdx", 2))
        self.assertEqual(len(got), 4)

    def test_empty_sentinel_bucket(self):
        df = pd.DataFrame(
            {"Agence": ["A", None, "", "A", "B", "   "]},
        )
        got = dict(fs.unique_values_in_column(df, "Agence"))
        self.assertEqual(got["A"], 2)
        self.assertEqual(got["B"], 1)
        # None + "" + "   " all bucket to empty sentinel (3).
        self.assertEqual(got[fs.EMPTY_RAW_KEY], 3)

    def test_unknown_column(self):
        df = pd.DataFrame({"A": [1, 2]})
        self.assertEqual(fs.unique_values_in_column(df, "B"), [])

    def test_empty_sentinel_always_last(self):
        df = pd.DataFrame({"c": ["a", None, "a", None, "a"]})
        got = fs.unique_values_in_column(df, "c")
        self.assertEqual(got[-1][0], fs.EMPTY_RAW_KEY)


class TestSuggestAgencyColumns(unittest.TestCase):
    def test_finds_common_variants(self):
        df = pd.DataFrame(columns=[
            "Plaque", "Agence", "Code CC", "Brand", "Centre de coût",
            "Etablissement", "Random",
        ])
        hits = fs.suggest_agency_columns(df)
        self.assertIn("Agence", hits)
        self.assertIn("Code CC", hits)
        self.assertIn("Centre de coût", hits)
        self.assertIn("Etablissement", hits)
        self.assertNotIn("Brand", hits)
        self.assertNotIn("Plaque", hits)


class TestBuildFleetMapping(unittest.TestCase):
    def _sample_source(self) -> pd.DataFrame:
        return pd.DataFrame({
            "Plaque": ["AB-123-CD", "EF-456-GH", "IJ-789-KL", "MN-012-OP"],
            "Agence": ["Bdx", "BORDEAUX", "Paris", None],
        })

    def test_merge_two_raws_into_same_fleet(self):
        """Bdx and BORDEAUX both → Bordeaux → 2 plates in Bordeaux."""
        df = self._sample_source()
        mapping = fs.build_fleet_mapping(
            source_file_key="client_file_1",
            source_column="Agence",
            raw_to_fleet={
                "Bdx": "Bordeaux",
                "BORDEAUX": "Bordeaux",
                "Paris": "Paris",
                fs.EMPTY_RAW_KEY: "Paris",  # user chose to send empties to Paris
            },
            source_df=df,
        )
        self.assertTrue(mapping.is_active)
        self.assertEqual(mapping.fleet_names, ["Bordeaux", "Paris"])
        counts = mapping.counts_by_fleet()
        self.assertEqual(counts["Bordeaux"], 2)
        # Paris = 1 explicit + 1 empty-mapped = 2
        self.assertEqual(counts["Paris"], 2)

    def test_ignored_raw_drops_plates(self):
        """If a raw is mapped to empty string, its plates are unassigned."""
        df = self._sample_source()
        mapping = fs.build_fleet_mapping(
            source_file_key="client_file_1",
            source_column="Agence",
            raw_to_fleet={
                "Bdx": "Bordeaux",
                "BORDEAUX": "Bordeaux",
                "Paris": "",  # ignored
                fs.EMPTY_RAW_KEY: "",  # ignored
            },
            source_df=df,
        )
        self.assertEqual(mapping.fleet_names, ["Bordeaux"])
        self.assertEqual(mapping.counts_by_fleet(), {"Bordeaux": 2})

    def test_case_insensitive_match(self):
        """Mapping built from unique list should still catch all rows even if
        the raw values differ in case/accents between popup and data."""
        df = pd.DataFrame({
            "Plaque": ["AB-123-CD", "EF-456-GH"],
            "Agence": ["Île-de-France", "ile de france"],
        })
        mapping = fs.build_fleet_mapping(
            source_file_key="client_file_1",
            source_column="Agence",
            raw_to_fleet={"Île-de-France": "IDF"},
            source_df=df,
        )
        self.assertEqual(mapping.counts_by_fleet(), {"IDF": 2})

    def test_no_plate_column_returns_empty_index(self):
        """No plate column → mapping keeps config but can't split anything."""
        df = pd.DataFrame({"Agence": ["Bdx"]})  # no plate column
        mapping = fs.build_fleet_mapping(
            source_file_key="x",
            source_column="Agence",
            raw_to_fleet={"Bdx": "Bordeaux"},
            source_df=df,
        )
        self.assertFalse(mapping.is_active)
        self.assertEqual(mapping.plate_to_fleet, {})


class TestSplitDfByFleet(unittest.TestCase):
    def test_no_mapping_returns_global_bucket(self):
        df = pd.DataFrame(
            {"registrationPlate": ["A", "B"]},
            index=["AB123CD", "EF456GH"],
        )
        out = fs.split_df_by_fleet(df, None)
        self.assertEqual(set(out.keys()), {""})
        self.assertEqual(len(out[""]), 2)

    def test_split_respects_index_membership(self):
        df = pd.DataFrame(
            {"col": [1, 2, 3, 4]},
            index=["P1", "P2", "P3", "P4"],
        )
        mapping = fs.FleetMapping(
            source_file_key="x",
            source_column="Agence",
            raw_to_fleet={"bdx": "Bordeaux", "paris": "Paris"},
            plate_to_fleet={"P1": "Bordeaux", "P2": "Bordeaux", "P3": "Paris"},
        )
        out = fs.split_df_by_fleet(df, mapping)
        self.assertEqual(len(out["Bordeaux"]), 2)
        self.assertEqual(len(out["Paris"]), 1)
        # P4 had no fleet → unassigned bucket.
        self.assertEqual(len(out[fs.UNASSIGNED_FLEET]), 1)


class TestSlugify(unittest.TestCase):
    def test_accents_stripped(self):
        self.assertEqual(fs.slugify_fleet_name("Île-de-France"), "ile-de-france")
        self.assertEqual(fs.slugify_fleet_name("Côte d'Azur"), "cote-d-azur")

    def test_spaces_and_symbols_collapsed(self):
        self.assertEqual(fs.slugify_fleet_name("Provence / PACA"), "provence-paca")

    def test_empty_input(self):
        self.assertEqual(fs.slugify_fleet_name(""), "")
        self.assertEqual(fs.slugify_fleet_name(None), "")

    def test_only_symbols_fallback(self):
        self.assertEqual(fs.slugify_fleet_name("!!!"), "fleet")


class TestSafeSheetName(unittest.TestCase):
    def test_truncates_and_cleans(self):
        long = "Agence" * 10  # 60 chars
        sn = fs.safe_sheet_name(long)
        self.assertLessEqual(len(sn), 31)

    def test_replaces_illegal_chars(self):
        sn = fs.safe_sheet_name("a/b?c*[d]e:f\\g")
        for bad in ["/", "?", "*", "[", "]", ":", "\\"]:
            self.assertNotIn(bad, sn)

    def test_fallback_on_empty(self):
        self.assertEqual(fs.safe_sheet_name(""), "Feuille")
        self.assertEqual(fs.safe_sheet_name(None), "Feuille")


if __name__ == "__main__":
    unittest.main()
