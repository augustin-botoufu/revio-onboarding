"""Smoke test end-to-end: à lancer avec `python test_smoke.py` depuis la racine
du projet pour vérifier que la chaîne fonctionne.

Prérequis :
  - venv activé
  - `pip install -r requirements.txt`
  - un fichier d'API Plaques quelque part (ajuste API_PLAQUES_PATH ci-dessous)

Ça fait :
  1. Charge un fichier API Plaques
  2. Applique un mapping manuel vers le schéma Vehicle
  3. Merge / valide / split par flotte / génère le ZIP
  4. Vérifie que l'en-tête du CSV correspond pile au template Revio
"""

import os
import sys
import io
import zipfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.pipeline import SourceFile, merge_per_schema, validate, load_tabular
from src.detectors import detect
from src.output_writer import split_by_fleet, build_zip
from src.schemas import header_for


# --> à adapter à l'emplacement d'un vrai fichier API Plaques chez toi
API_PLAQUES_PATH = "../uploads/Fichier import - API PLaque.csv"


class FakeFile:
    def __init__(self, path):
        self.path = path
        self.name = os.path.basename(path)

    def read(self):
        return open(self.path, "rb").read()


def main():
    if not os.path.exists(API_PLAQUES_PATH):
        print(f"Fichier manquant: {API_PLAQUES_PATH}")
        print("Édite API_PLAQUES_PATH en haut du script.")
        sys.exit(1)

    api_file = FakeFile(API_PLAQUES_PATH)
    sheet_name, df_api = load_tabular(api_file)[0]
    det_api = detect(api_file.name, df_api)
    print("Détection:", det_api.source_type, f"(conf={det_api.confidence})")

    sf_api = SourceFile(
        key="api",
        filename=api_file.name,
        sheet_name=sheet_name,
        df_raw=df_api,
        detected=det_api,
        target_schema="vehicle",
        mapping={
            "registrationPlate": "immatriculation",
            "registrationVin": "vin",
            "brand": "marque",
            "model": "modele",
            "bodyDescription": "carrosserieCG",
            "registrationIssueDate": "date1erCir_fr",
            "co2gKm": "co2",
        },
        selected=True,
    )

    merged = merge_per_schema(
        [sf_api],
        priority_order=["api_plaques", "ayvens_etat_parc", "arval_uat", "client_vehicle"],
    )
    print("Schémas produits:", list(merged.keys()))
    for k, v in merged.items():
        print("  ", k, "shape", v.shape)

    issues = validate(merged)
    print("Issues de validation:", len(issues))
    if issues:
        print("Ex.:", issues[0].schema, issues[0].plate, "->", issues[0].message)

    by_fleet = split_by_fleet(merged, {})
    zip_bytes = build_zip(by_fleet, issues, client_name="TEST")
    print("ZIP:", len(zip_bytes), "octets")

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if "vehicle" in name:
                content = zf.read(name).decode("utf-8-sig")
                header_line = content.splitlines()[0]
                expected = ",".join(header_for("vehicle"))
                assert header_line == expected, (
                    f"Header mismatch!\n  got:      {header_line}\n  expected: {expected}"
                )
                print("Header vehicle.csv: OK (match exact avec le template Revio)")

    print("\nOK, tout passe.")


if __name__ == "__main__":
    main()
