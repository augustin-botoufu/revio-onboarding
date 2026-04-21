"""CLI test of the Vehicle rules engine on a fake 3-vehicle dataset.

Usage:
    cd revio_onboarding
    python3 test_rules_engine.py

Simulates 3 cars, 3 sources:
  - API Plaques (exhaustive, official SIV data)
  - Ayvens - État de parc (lessor file for 2 of the 3 cars)
  - Fichier véhicules client (interne) (client file for all 3 cars,
    with a 'usage' column that only the client has)

The test asserts that the merged Vehicle output has the expected values
on each field, respecting the priorities declared in vehicle.yml.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Ensure local package is importable when run from the repo root
sys.path.insert(0, str(Path(__file__).parent))

from src import rules_engine  # noqa: E402


# ---------- Fake test data ----------

# 3 cars: AB-123-CD (Renault Clio diesel), EF-456-GH (Peugeot 208 electric),
# IJ-789-KL (Tesla Model 3 electric).
# Car 2 (EF-456-GH) is NOT in Ayvens (assume the client owns it / another lessor).

API_PLAQUES = pd.DataFrame([
    {
        "immatriculation": "AB123CD",  # no dashes, should be normalized
        "vin": " vf1br0f0566123456 ",  # spaces + lowercase
        "marque": "RENAULT",
        "modele": "CLIO",
        "version": "Intens",
        "energieNGC": "GO",  # diesel
        "genreVCGNGC": "VP",  # private
        "co2": 105,
        "puisFisc": 5,
        "date_premiere_circulation": "2022-03-15",
        "poids": 1200,
        "photo_modele": "https://photos.api/clio.jpg",
    },
    {
        "immatriculation": "EF-456-GH",
        "vin": "VF3XXXXXXXXXXXXXX",  # 17 chars
        "marque": "PEUGEOT",
        "modele": "208",
        "version": "GT",
        "energieNGC": "EL",  # electric
        "genreVCGNGC": "VP",
        "co2": 0,
        "puisFisc": 3,
        "date_premiere_circulation": "2023-07-01",
        "poids": 1450,
        "puissance_nette_max": 100,
        "photo_modele": "https://photos.api/208.jpg",
    },
    {
        "immatriculation": "IJ-789-KL",
        "vin": "5YJ3E1EA7NF000123",
        "marque": "TESLA",
        "modele": "MODEL 3",
        "version": "Long Range",
        "energieNGC": "EL",
        "genreVCGNGC": "VP",
        "co2": 0,
        "puisFisc": 4,
        "date_premiere_circulation": "2024-01-10",
        "poids": 1850,
        "puissance_nette_max": 324,
        "photo_modele": "https://photos.api/model3.jpg",
    },
])

# Ayvens has cars 1 & 3 + 1 "ghost" car (XY-999-ZZ) that was restitued / is
# an external contract and should NOT end up in the client parc.
AYVENS_ETAT_PARC = pd.DataFrame([
    {
        "N° immat": "AB-123-CD",
        "N° châssis": "VF1BR0F0566123456",
        "Marque": "Renault",
        "Gamme": "Clio",
        "Version": "Intens TCe 90",
        "Energie": "Gazole",
        "Genre": "particulier",
        "CO2": 108,  # divergent from API Plaques (105) — API wins (prio 1)
        "CV fiscaux": 5,
        "Date 1ère MEC": "15/03/2022",
        "Date début contrat": "2022-03-20",  # becomes parcEntryAt (prio 1 for dates)
    },
    {
        "N° immat": "IJ-789-KL",
        "N° châssis": "5YJ3E1EA7NF000123",
        "Marque": "Tesla",
        "Gamme": "Model 3",
        "Version": "Long Range",
        "Energie": "Electrique",
        "Genre": "particulier",
        "CO2": 0,
        "CV fiscaux": 4,
        "Date 1ère MEC": "10/01/2024",
        "Date début contrat": "2024-01-15",
    },
    {
        # Ghost car: in Ayvens but NOT in the client file.
        # Expected behavior: NOT in the output DataFrame, but flagged in issues.
        "N° immat": "XY-999-ZZ",
        "N° châssis": "VF9GHOST000000000",
        "Marque": "Citroen",
        "Gamme": "C3",
        "Version": "Shine",
        "Energie": "Essence",
        "Genre": "particulier",
        "CO2": 120,
        "CV fiscaux": 5,
        "Date 1ère MEC": "01/06/2021",
        "Date début contrat": "2021-06-10",
    },
])

# Client file — has ALL 3 cars + 'usage' column with business logic
# (client declares "fonction" for the Tesla — will map to 'service')
CLIENT_FILE = pd.DataFrame([
    {
        "plaque": "AB-123-CD",
        "usage_client": "particulier",  # private
        "date_entree_parc": "2022-03-20",
    },
    {
        "plaque": "EF-456-GH",
        "usage_client": "fonction",  # service
        "date_entree_parc": "2023-07-10",
    },
    {
        "plaque": "IJ-789-KL",
        "usage_client": "particulier",
        "date_entree_parc": "2024-01-15",
    },
])


# ---------- Run engine ----------

def main():
    sources = {
        "api_plaques": API_PLAQUES,
        "ayvens_etat_parc": AYVENS_ETAT_PARC,
        "client_file": CLIENT_FILE,
    }
    # The 'client_file' needs manual column overrides for plate/usage/parcEntryAt
    # (its column names are user-chosen and don't match the YAML).
    overrides = {
        ("client_file", "registrationPlate"): "plaque",
        ("client_file", "usage"): "usage_client",
        ("client_file", "parcEntryAt"): "date_entree_parc",
    }
    result = rules_engine.run_vehicle(sources, manual_column_overrides=overrides)
    df = result.df

    print("=" * 80)
    print(" VEHICLE OUTPUT (merged)")
    print("=" * 80)
    print(df.to_string())
    print()
    print("=" * 80)
    print(f" ISSUES ({len(result.issues)})")
    print("=" * 80)
    for issue in result.issues[:30]:
        print(f"  [{issue.plate}] field={issue.field} source={issue.source} — {issue.warning}")
    if len(result.issues) > 30:
        print(f"  ... and {len(result.issues) - 30} more.")

    # ---------- Assertions ----------
    # Option 3 (hybride): only client_file plates make it to the output.
    # Ayvens has 3 cars but XY-999-ZZ isn't in the client file → excluded + flagged.
    assert len(df) == 3, f"Expected 3 rows (client file size), got {len(df)}"
    assert "XY999ZZ" not in df.index, "Ghost Ayvens plate should NOT be in output"

    # The ghost plate must appear in issues with the expected warning
    ghost_issues = [i for i in result.issues if i.plate == "XY999ZZ"]
    assert len(ghost_issues) >= 1, "Ghost plate should be flagged in issues"
    assert "absente du fichier client" in ghost_issues[0].warning, ghost_issues[0].warning

    # Car 1 (AB-123-CD): API Plaques wins on most fields, client wins on plate,
    # but normalize_plate produces AB-123-CD. The client_file plate value is
    # already "AB-123-CD" so both give the same result.
    car1 = df.loc["AB123CD"]
    assert car1["registrationPlate"] == "AB-123-CD", car1["registrationPlate"]
    assert car1["usage"] == "private", f"usage should be 'private' (from API Plaques VP), got {car1['usage']!r}"
    assert car1["brand"] == "RENAULT", car1["brand"]
    assert car1["model"] == "CLIO", car1["model"]
    assert car1["motorisation"] == "diesel", car1["motorisation"]
    assert car1["co2gKm"] == 105, f"Expected 105 (API Plaques), got {car1['co2gKm']} — priority ordering bug"
    # parcEntryAt: Ayvens wins (prio 1) over client (prio 2) — both same value here
    assert car1["parcEntryAt"] == "2022/03/20", f"parcEntryAt: {car1['parcEntryAt']}"
    assert car1["registrationIssueDate"] == "2022/03/15", car1["registrationIssueDate"]
    assert car1["registrationVin"] == "VF1BR0F0566123456", car1["registrationVin"]
    assert car1["registrationIssueCountryCode"] == "FR"
    assert car1["imageUrl"] == "https://photos.api/clio.jpg"

    # Car 2 (EF-456-GH): only API Plaques + client. No Ayvens.
    car2 = df.loc["EF456GH"]
    assert car2["registrationPlate"] == "EF-456-GH", car2["registrationPlate"]
    assert car2["motorisation"] == "electric", car2["motorisation"]
    assert car2["usage"] == "private", car2["usage"]
    # parcEntryAt: no Ayvens on this car → fallback to client (prio 2)
    assert car2["parcEntryAt"] == "2023/07/10", f"parcEntryAt fallback failed: {car2['parcEntryAt']}"
    assert car2["electricEnginePower"] == 100, car2["electricEnginePower"]

    # Car 3 (IJ-789-KL): Tesla
    car3 = df.loc["IJ789KL"]
    assert car3["registrationPlate"] == "IJ-789-KL"
    assert car3["brand"] == "TESLA"
    assert car3["motorisation"] == "electric"

    print()
    print("All assertions passed ✔")


if __name__ == "__main__":
    main()
