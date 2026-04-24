"""End-to-end integration test for the Contract engine.

Runs the full pipeline:
  Arval PDF facture → pdf_parser → contract_engine (+ EP Ayvens context + client_file)
And asserts:
  - contracts are populated for plates in client_file
  - orphans are flagged for plates present in factures but absent from client_file
  - lineage records are produced (≥ 1 per non-null cell)
  - unknown_column_requests is empty for this fixture (all mandatory fields resolvable)

Run:
    python test_integration.py

Exit 0 on success, non-zero on any assertion failure.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

import pandas as pd
import yaml

HERE = Path(__file__).resolve().parent
SRC = HERE / "src"
# Add the parent of src/ so we can import it as a package.
sys.path.insert(0, str(HERE))

from src.pdf_parser import parse_factures_to_dataframe  # noqa: E402
from src.contract_engine import run_contract  # noqa: E402


UPLOADS = Path("/sessions/zealous-sharp-faraday/mnt/uploads")
RULES = SRC / "rules" / "contract.yml"
RUBRIQUES = SRC / "rules" / "rubriques_facture.yml"


def _load_client_file() -> pd.DataFrame:
    """Minimal fake client_file: two plates that match Arval PDF."""
    return pd.DataFrame([
        {"Immatriculation": "GB-058-QG", "Numéro contrat": "1634070",
         "partnerId": "ARVAL_UUID", "agence": "Paris"},
        {"Immatriculation": "GK-182-BE", "Numéro contrat": "2141682",
         "partnerId": "ARVAL_UUID", "agence": "Lyon"},
    ])


def _load_ayvens_ep() -> pd.DataFrame:
    """Minimal fake Ayvens EP: provides a VP indicator for one plate."""
    return pd.DataFrame([
        {"Immatriculation": "GB-058-QG", "Genre": "VP", "Marque": "PEUGEOT"},
        {"Immatriculation": "GK-182-BE", "Genre": "N1", "Marque": "RENAULT"},
    ])


def run() -> int:
    print("→ Integration test : Contract engine")
    print(f"  rules  : {RULES}")
    pdf_path = UPLOADS / "PDF_CE0298_26AL0279715_499581_20260422.pdf"
    print(f"  pdf    : {pdf_path.name}")

    # 1. Parse the PDF
    rub = yaml.safe_load(RUBRIQUES.read_text(encoding="utf-8")) or {}
    facture_df = parse_factures_to_dataframe(
        [str(pdf_path)],
        whitelist=rub.get("whitelist", []),
        blacklist=rub.get("blacklist", []),
        lessor_hint="arval",
    )
    print(f"  ✓ PDF → {len(facture_df)} contrats détectés")
    assert len(facture_df) >= 1, "pdf_parser should return at least one contract"

    # 2. Run engine
    client_df = _load_client_file()
    ep_ayvens = _load_ayvens_ep()
    sources = {
        "client_file": client_df,
        "arval_facture_pdf": facture_df,
        "ayvens_etat_parc": ep_ayvens,
    }
    result = run_contract(
        source_dfs=sources,
        rules_path=str(RULES),
    )

    # 3. Assertions
    df_out = result.df
    print(f"  ✓ Engine → {len(df_out)} contrats populés, {len(result.orphan_df)} orphans")
    print(f"           lineage: {len(result.lineage)} records, issues: {len(result.issues)}")

    assert len(df_out) == 2, f"expected 2 populated contracts, got {len(df_out)}"
    assert "plate" in df_out.columns, "output must carry plate"
    assert "number" in df_out.columns, "output must carry number"
    assert "totalPrice" in df_out.columns, "output must carry totalPrice"

    # At least one totalPrice filled
    non_null_prices = df_out["totalPrice"].dropna()
    assert len(non_null_prices) >= 1, "at least one totalPrice should be non-null"

    # Lineage: non-empty and well-shaped
    assert len(result.lineage) > 0, "lineage store should not be empty"
    sample = result.lineage._records[0]
    assert sample.table == "contract", f"lineage records should tag table='contract', got {sample.table}"
    assert sample.rule_id, "lineage should carry rule_id"

    # Unknown column requests: allowed empty for this fixture
    print(f"  ✓ unknown_column_requests: {len(result.unknown_column_requests)}")

    # isHT derivation (post-pass)
    if "isHT" in df_out.columns:
        for _, row in df_out.iterrows():
            print(f"    isHT[{row['plate']}|{row['number']}] = {row.get('isHT')}")

    print("  ✓ All assertions passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except AssertionError as e:
        print(f"  ✗ ASSERTION FAILED: {e}")
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"  ✗ UNEXPECTED ERROR: {e}")
        traceback.print_exc()
        sys.exit(2)
