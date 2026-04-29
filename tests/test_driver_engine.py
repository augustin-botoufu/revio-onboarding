"""Unit tests for src/driver_engine.py (Jalon 5.1).

Covers :
- civility normalisation (every major variant + unknowns)
- license expiry rule (all 4 cases of the truth table)
- plate key normalisation (hyphens / case / spaces)
- anomaly detection (unknown plate, duplicates)
- shape detection (fingerprint)
- end-to-end on the real YSEIS LIL01 file the user uploaded

Run:
    python -m unittest tests/test_driver_engine.py -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import driver_engine as de


# =============================================================================
# Civility
# =============================================================================


class TestCivility(unittest.TestCase):

    def test_numeric_pass_through(self):
        self.assertEqual(de.normalize_civility("1"), ("1", None))
        self.assertEqual(de.normalize_civility("2"), ("2", None))
        self.assertEqual(de.normalize_civility(1), ("1", None))
        self.assertEqual(de.normalize_civility(2), ("2", None))

    def test_male_variants(self):
        for v in ["M", "M.", "Mr", "Mr.", "Monsieur", "Messieurs",
                  "H", "Homme", "male", "MAN", "masculin"]:
            val, warn = de.normalize_civility(v)
            self.assertEqual(val, "1", msg=f"{v!r} should map to 1")
            self.assertIsNone(warn)

    def test_female_variants(self):
        for v in ["Mme", "Mme.", "Mrs", "Mrs.", "Madame", "Mesdames",
                  "F", "Femme", "female", "woman", "FÉMININ", "feminin"]:
            val, warn = de.normalize_civility(v)
            self.assertEqual(val, "2", msg=f"{v!r} should map to 2")
            self.assertIsNone(warn)

    def test_blank_is_not_warning(self):
        for v in [None, "", "   ", float("nan")]:
            val, warn = de.normalize_civility(v)
            self.assertIsNone(val)
            self.assertIsNone(warn)

    def test_unknown_emits_warning(self):
        val, warn = de.normalize_civility("Mx")
        self.assertIsNone(val)
        self.assertIsNotNone(warn)
        self.assertIn("Mx", warn)


# =============================================================================
# License expiry rule
# =============================================================================


class TestLicenseExpiry(unittest.TestCase):

    def test_all_four_cases(self):
        # blank + blank → blank
        self.assertEqual(de.apply_license_expiry_rule("", ""), "")
        self.assertEqual(de.apply_license_expiry_rule(None, None), "")
        # blank licence + real expiry → keep expiry (no reason to touch)
        self.assertEqual(
            de.apply_license_expiry_rule("", "2030/01/01"),
            "2030/01/01",
        )
        # licence + blank expiry → 2033/01/19 (old FR permit)
        self.assertEqual(
            de.apply_license_expiry_rule("120659501108", ""),
            "2033/01/19",
        )
        self.assertEqual(
            de.apply_license_expiry_rule("120659501108", None),
            "2033/01/19",
        )
        # licence + real expiry → pass through
        self.assertEqual(
            de.apply_license_expiry_rule("120659501108", "2030/01/01"),
            "2030/01/01",
        )

    def test_custom_default(self):
        # Gives us a hook to override from YAML later without breaking call sites.
        self.assertEqual(
            de.apply_license_expiry_rule("ABC", "", default="2040/12/31"),
            "2040/12/31",
        )


# =============================================================================
# Plate key
# =============================================================================


class TestPlateKey(unittest.TestCase):

    def test_various_formats_canonicalise_same(self):
        self.assertEqual(de.plate_key("GL-536-GZ"), "GL536GZ")
        self.assertEqual(de.plate_key("gl-536-gz"), "GL536GZ")
        self.assertEqual(de.plate_key(" GL536GZ "), "GL536GZ")
        self.assertEqual(de.plate_key("gl 536 gz"), "GL536GZ")

    def test_empty(self):
        self.assertEqual(de.plate_key(""), "")
        self.assertEqual(de.plate_key(None), "")
        self.assertEqual(de.plate_key(float("nan")), "")


# =============================================================================
# Shape detection
# =============================================================================


class TestIsDriverShape(unittest.TestCase):

    def test_matches_full_template(self):
        cols = list(de.DRIVER_COLUMNS_EXPECTED)
        self.assertTrue(de.is_driver_shape(cols))

    def test_matches_real_yseis_columns(self):
        cols = [
            "firstName", "lastName", "civility", "birthDate", "birthCity",
            "emailPro", "emailPerso", "phone", "street", "city", "postalCode",
            "countryCode", "seniority", "professionalStatus", "licenseNumber",
            "licenseIssueCountryCode", "licenseIssueLocation",
            "licenseIssueDate", "licenseExpiryDate", "assignPlate",
            "registrationIssueCountryCode", "assignFrom", "assignTo",
            "companyAnalyticalCode", "locationId",
        ]
        self.assertTrue(de.is_driver_shape(cols))

    def test_rejects_vehicle_file(self):
        cols = ["registrationPlate", "brand", "model", "parcEntryAt", "registrationVin"]
        self.assertFalse(de.is_driver_shape(cols))

    def test_case_insensitive(self):
        cols = ["FIRSTNAME", "LASTNAME", "emailpro", "LICENSENUMBER", "assignplate"]
        self.assertTrue(de.is_driver_shape(cols))


# =============================================================================
# Anomaly detection
# =============================================================================


class TestAnomalies(unittest.TestCase):

    def _mk(self, rows):
        return pd.DataFrame(rows)

    def test_unknown_plate_raises_anomaly(self):
        df = self._mk([
            {"firstName": "A", "lastName": "A", "assignPlate": "AB-123-CD",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "1"},
            {"firstName": "B", "lastName": "B", "assignPlate": "EF-456-GH",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "2"},
        ])
        # Only AB-123-CD exists in the vehicle file.
        result = de.process_drivers(df, vehicle_plates=["AB-123-CD"])
        codes = [a.code for a in result.anomalies]
        self.assertIn("assign_plate_not_in_vehicles", codes)
        # The anomaly points at row 2 (B), not row 1 (A).
        unknowns = [a for a in result.anomalies if a.code == "assign_plate_not_in_vehicles"]
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0].row, 2)
        self.assertEqual(unknowns[0].plate, "EF-456-GH")
        self.assertIn("B B", unknowns[0].driver)

    def test_duplicate_plate_raises_anomaly_for_every_driver(self):
        df = self._mk([
            {"firstName": "A", "lastName": "A", "assignPlate": "AB-123-CD",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "1"},
            {"firstName": "B", "lastName": "B", "assignPlate": "AB-123-CD",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "2"},
            {"firstName": "C", "lastName": "C", "assignPlate": "XY-999-ZZ",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "1"},
        ])
        result = de.process_drivers(df, vehicle_plates=["AB-123-CD", "XY-999-ZZ"])
        dups = [a for a in result.anomalies if a.code == "assign_plate_duplicated"]
        # Both A and B are flagged, C is not.
        self.assertEqual(len(dups), 2)
        rows = sorted(a.row for a in dups)
        self.assertEqual(rows, [1, 2])
        # The message on each row lists both drivers.
        for d in dups:
            self.assertIn("A A", d.message)
            self.assertIn("B B", d.message)

    def test_vehicle_plates_none_skips_unknown_check(self):
        df = self._mk([
            {"firstName": "A", "lastName": "A", "assignPlate": "ZZ-999-ZZ",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "1"},
        ])
        result = de.process_drivers(df, vehicle_plates=None)
        self.assertEqual(result.anomalies, [])

    def test_hyphen_case_dont_trigger_unknown(self):
        """Plate comparison must be format-agnostic."""
        df = self._mk([
            {"firstName": "A", "lastName": "A", "assignPlate": "ab-123-cd",
             "licenseNumber": "", "licenseExpiryDate": "", "civility": "1"},
        ])
        result = de.process_drivers(df, vehicle_plates=["AB123CD"])
        self.assertEqual(result.anomalies, [])


# =============================================================================
# Full pipeline
# =============================================================================


class TestProcessDrivers(unittest.TestCase):

    def test_civility_and_license_expiry_are_applied(self):
        df = pd.DataFrame([
            {"firstName": "A", "lastName": "A", "civility": "Monsieur",
             "licenseNumber": "123", "licenseExpiryDate": "",
             "assignPlate": ""},
            {"firstName": "B", "lastName": "B", "civility": "MADAME",
             "licenseNumber": "", "licenseExpiryDate": "",
             "assignPlate": ""},
        ])
        result = de.process_drivers(df, vehicle_plates=[])
        self.assertEqual(list(result.df["civility"]), ["1", "2"])
        self.assertEqual(
            list(result.df["licenseExpiryDate"]),
            [de.FR_LEGACY_LICENSE_EXPIRY, ""],
        )
        self.assertEqual(result.warnings, [])  # both civility values known

    def test_unknown_civility_produces_warning_and_clears_cell(self):
        df = pd.DataFrame([
            {"firstName": "A", "lastName": "A", "civility": "Mx",
             "licenseNumber": "", "licenseExpiryDate": "",
             "assignPlate": ""},
        ])
        result = de.process_drivers(df)
        self.assertIsNone(result.df.iloc[0]["civility"])
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(result.warnings[0].code, "civility_unrecognised")

    def test_yseis_lil01_real_file_clean(self):
        """The uploaded YSEIS LIL01 file should pass through without
        anomalies when its own 4 plates are the Vehicle file."""
        path = ROOT / "tests" / "fixtures" / "yseis_lil01_driver.csv"
        if not path.exists():
            self.skipTest(f"Fixture missing: {path}")
        df = pd.read_csv(path)
        own_plates = df["assignPlate"].dropna().tolist()
        result = de.process_drivers(df, vehicle_plates=own_plates)
        self.assertEqual(result.n_anomalies, 0, msg=str([a.as_record() for a in result.anomalies]))
        # All 4 civility values are already 1 or 2 → no warnings.
        self.assertEqual(len(result.warnings), 0)
        # The 3 pre-2013 plates keep their "2033/01/19" expiry, the 4th keeps its real one.
        self.assertIn("2033/01/19", list(result.df["licenseExpiryDate"]))
        self.assertIn("2040/09/29", list(result.df["licenseExpiryDate"]))

    def test_empty_dataframe_returns_clean(self):
        result = de.process_drivers(pd.DataFrame())
        self.assertEqual(result.n_drivers, 0)
        self.assertEqual(result.anomalies, [])


# =============================================================================
# extract_vehicle_plates
# =============================================================================


class TestExtractVehiclePlates(unittest.TestCase):

    def test_prefers_registrationPlate(self):
        df = pd.DataFrame({
            "registrationPlate": ["AB-123-CD", "EF-456-GH"],
            "plate": ["ignored", "ignored"],
        })
        self.assertEqual(
            de.extract_vehicle_plates(df),
            ["AB-123-CD", "EF-456-GH"],
        )

    def test_fallback_to_plate(self):
        df = pd.DataFrame({"plate": ["AB-123-CD"]})
        self.assertEqual(de.extract_vehicle_plates(df), ["AB-123-CD"])

    def test_empty_df_returns_empty(self):
        self.assertEqual(de.extract_vehicle_plates(pd.DataFrame()), [])
        self.assertEqual(de.extract_vehicle_plates(None), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
