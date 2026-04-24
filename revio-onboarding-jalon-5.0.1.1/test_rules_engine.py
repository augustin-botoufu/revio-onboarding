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
        "N° Immat": "AB-123-CD",
        "N° de chassis": "VF1BR0F0566123456",
        "Marque": "Renault",
        "Gamme": "Clio",
        "Version": "Intens TCe 90",
        "Energie": "Gazole",
        "Genre": "particulier",
        "CO² (g/km)":108,  # divergent from API Plaques (105) — API wins (prio 1)
        "CV Fisc.": 5,
        "1ère MEC": "15/03/2022",
        "Date début contrat": "2022-03-20",  # becomes parcEntryAt (prio 1 for dates)
    },
    {
        "N° Immat": "IJ-789-KL",
        "N° de chassis": "5YJ3E1EA7NF000123",
        "Marque": "Tesla",
        "Gamme": "Model 3",
        "Version": "Long Range",
        "Energie": "Electrique",
        "Genre": "particulier",
        "CO² (g/km)":0,
        "CV Fisc.": 4,
        "1ère MEC": "10/01/2024",
        "Date début contrat": "2024-01-15",
    },
    {
        # Ghost car: in Ayvens but NOT in the client file.
        # Expected behavior: NOT in the output DataFrame, but flagged in issues.
        "N° Immat": "XY-999-ZZ",
        "N° de chassis": "VF9GHOST000000000",
        "Marque": "Citroen",
        "Gamme": "C3",
        "Version": "Shine",
        "Energie": "Essence",
        "Genre": "particulier",
        "CO² (g/km)":120,
        "CV Fisc.": 5,
        "1ère MEC": "01/06/2021",
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
    print(f" ORPHAN PLATES ({0 if result.orphan_df is None else len(result.orphan_df)})")
    print("=" * 80)
    if result.orphan_df is not None:
        print(result.orphan_df.to_string())
    print()
    print("=" * 80)
    print(f" CONFLICTS ({len(result.conflicts_by_cell)})")
    print("=" * 80)
    for (plate, field_name), conflicts in list(result.conflicts_by_cell.items())[:20]:
        parts = " vs ".join(f"{s}={v}" for s, v in conflicts)
        print(f"  [{plate}] {field_name}: {parts}")
    print()
    print("=" * 80)
    print(f" GLOBAL ISSUES ({len(result.issues)})")
    print("=" * 80)
    for issue in result.issues[:30]:
        print(f"  [{issue.plate}] field={issue.field} source={issue.source} — {issue.warning}")

    # ---------- Assertions ----------
    # Option 3 (hybride): only client_file plates make it to the output.
    # Ayvens has 3 cars but XY-999-ZZ isn't in the client file → excluded
    # and moved to orphan_df.
    assert len(df) == 3, f"Expected 3 rows (client file size), got {len(df)}"
    assert "XY999ZZ" not in df.index, "Ghost Ayvens plate should NOT be in output"

    # The ghost plate must appear in orphan_df, enriched with Ayvens data
    assert result.orphan_df is not None, "orphan_df should be populated"
    assert "XY999ZZ" in result.orphan_df.index, "Ghost plate should be in orphan_df"
    ghost = result.orphan_df.loc["XY999ZZ"]
    assert ghost["brand"] == "CITROEN", f"orphan brand: {ghost['brand']}"
    assert ghost["model"] == "C3", f"orphan model: {ghost['model']}"
    assert "ayvens_etat_parc" in ghost["sources_found"], ghost["sources_found"]

    # Conflict tracking: car 1 has a CO2 conflict (API=105 vs Ayvens=108)
    car1_co2_conflict = result.conflicts_by_cell.get(("AB123CD", "co2gKm"))
    assert car1_co2_conflict is not None, (
        "Expected a conflict on (AB123CD, co2gKm): API=105 vs Ayvens=108"
    )
    winner = car1_co2_conflict[0]
    # Both api_plaques and ayvens_etat_parc are now at priority 1 for co2gKm;
    # deterministic tie-break = source name alphabetical → api_plaques wins.
    assert winner[0] == "api_plaques" and winner[1] == 105, winner
    others = [c for c in car1_co2_conflict[1:] if c[0] == "ayvens_etat_parc"]
    assert others and others[0][1] == 108, car1_co2_conflict

    # Source tracking: car 1 co2 winner is api_plaques
    assert result.source_by_cell.get(("AB123CD", "co2gKm")) == "api_plaques"
    # parcEntryAt for car 1 should come from ayvens_etat_parc (prio 1)
    assert result.source_by_cell.get(("AB123CD", "parcEntryAt")) == "ayvens_etat_parc"
    # parcEntryAt for car 2 should fall back to client_file (no Ayvens on car 2)
    assert result.source_by_cell.get(("EF456GH", "parcEntryAt")) == "client_file"
    # Country code for every plate is set by the __default__ rule
    assert result.source_by_cell.get(("AB123CD", "registrationIssueCountryCode")) == "__default__"

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
