"""Parse lessor invoice PDFs into a structured DataFrame.

Feeds the Contract rules engine with P2-level contributions to
totalPrice + detailed prices (civilLiability, allRisks, etc.) and
cross-checks vs état-de-parc values (tolerance 2% + 2€ — see R3).

Architecture
------------
- Dispatch by lessor. Each lessor's facture has its own layout, so
  we keep a parser class per lessor (ArvalFactureParser today,
  AyvensFactureParser + AutreLoueurFactureParser as stubs to extend
  when we receive samples).
- Common output: a list of `ContractBlock` entries, one per contract
  found in the facture. Each block carries plate, number, metadata,
  and a list of rubriques (label + amount HT + amount TTC).
- `classify_rubriques()` applies the whitelist / blacklist from
  rules/rubriques_facture.yml to each rubrique, producing a
  `facture_row` dict suitable for the rules engine.
- `parse_multiple()` concatenates multiple factures (typical: one PDF
  per month) and dedups by (plate, number) keeping the MOST RECENT
  facture per R4 of the spec.

Public API
----------
    from .pdf_parser import parse_factures_to_dataframe
    df = parse_factures_to_dataframe(
        pdf_paths=["facture_arval_2026-03.pdf", "facture_arval_2026-04.pdf"],
        rubriques_yml=load_rubriques(),
        lessor_hint="arval",  # or None for auto-detect
    )
    # df columns: plate, number, facture_date, durationMonths, maxMileage,
    # restitutionDate, startDate, endDate, totalPrice, civilLiabilityPrice,
    # allRisksPrice, theftFireAndGlassPrice, financialLossPrice,
    # legalProtectionPrice, maintenancePrice, replacementVehiclePrice,
    # tiresPrice, gasCardPrice, tollCardPrice, _unknown_rubriques

No OCR. Requires text-extractable PDFs (the loueurs we've seen always
generate text PDFs). If pdfplumber fails, pypdf is tried as fallback.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd


# ---------- Data classes ----------


@dataclass
class Rubrique:
    label: str
    ht: Optional[float] = None
    ttc: Optional[float] = None
    # Populated by classify_rubriques()
    revio_field: Optional[str] = None
    contributes_to_total_only: bool = False
    blacklisted: bool = False
    unknown: bool = False


@dataclass
class ContractBlock:
    plate: str
    number: str
    lessor: str
    facture_date: Optional[str] = None     # YYYY/MM/DD
    duration_months: Optional[int] = None
    max_mileage: Optional[int] = None
    restitution_date: Optional[str] = None  # YYYY/MM/DD
    start_date: Optional[str] = None        # YYYY/MM/DD (period "Du")
    end_date: Optional[str] = None          # YYYY/MM/DD (period "au")
    rubriques: list[Rubrique] = field(default_factory=list)
    raw_text: str = ""


# ---------- Extraction backend ----------


def _extract_text(path: str | Path) -> str:
    """Extract text from a PDF. Try pdfplumber first, fallback to pypdf."""
    path = Path(path)
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(path) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception:
        pass
    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(str(path))
        return "\n".join((p.extract_text() or "") for p in reader.pages)
    except Exception as e:
        raise RuntimeError(
            f"Impossible d'extraire le texte du PDF {path}. "
            f"Vérifier qu'il n'est pas scanné (OCR non supporté). Cause : {e}"
        )


# ---------- Helpers ----------


_PRICE = r"-?\d{1,3}(?:[\s\u00a0]\d{3})*(?:[.,]\d{1,2})?"


def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    t = s.replace("\u00a0", "").replace(" ", "").replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def _fr_to_iso(date_str: str) -> Optional[str]:
    """DD/MM/YYYY → YYYY/MM/DD. Returns None if unparseable."""
    if not date_str:
        return None
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str.strip())
    if not m:
        return None
    d, mo, y = m.groups()
    return f"{y}/{mo}/{d}"


def _normalize_plate(raw: str) -> str:
    """Loose plate normalization for cross-keying. Strips spaces/dashes,
    uppercases. Upstream will do stricter format checks."""
    return re.sub(r"[\s\-]+", "", (raw or "").strip().upper())


# ---------- Arval parser ----------


class ArvalFactureParser:
    """Parser for Arval 'Document / Facture' PDFs.

    Based on the sample `PDF_CE0298_26AL0279715_499581_20260422.pdf`.
    Handles pages with an 'Annexe Détaillée' listing per-contract blocks.
    """

    lessor = "arval"

    # Facture number + facture date (page 1)
    FACTURE_NO_RE = re.compile(r"FACTURE\s+N°\s*([A-Z0-9]+)", re.I)
    FACTURE_DATE_RE = re.compile(
        r",\s+le\s+(\d{1,2})\s+([a-zéûôîèàç]+)\s+(\d{4})", re.I
    )

    # Contract header (can span multiple text lines after pdf reflow).
    # We instead detect the "Contrat N° XXXX - PLATE - ..." marker and
    # slice the text between two such markers.
    CONTRACT_HEADER_RE = re.compile(
        r"Contrat\s+N°\s*(?P<number>\d+)\s*-\s*(?P<plate>[A-Z0-9\-]+)\s*-",
        re.I,
    )
    DURATION_RE = re.compile(r"(\d+)\s*M\s*/\s*([\d\s]+)\s*Km", re.I)
    RESTIT_RE = re.compile(
        r"Date\s+de\s+restit\.?\s+prévue\s*:\s*(\d{2}/\d{2}/\d{4})", re.I
    )
    PERIOD_RE = re.compile(
        r"Du\s+(\d{2}/\d{2}/\d{4})\s+au\s+(\d{2}/\d{2}/\d{4})", re.I
    )
    # Rubrique line: <label> <prixU> <qty> <HT> <taux> <TVA> <TTC>
    # Or with H.C. (hors champ TVA): <label> <prixU> <qty> <HT> H.C. <vide> <TTC>
    # We match the TAIL first (HT … TTC), then the label is whatever comes before.
    RUBRIQUE_TAIL_RE = re.compile(
        r"(?P<ht>" + _PRICE + r")\s+(?:(?P<taux>\d{1,2}\.\d{1,2})|H\.C\.)"
        r"(?:\s+(?P<tva>" + _PRICE + r"))?\s+(?P<ttc>" + _PRICE + r")\s*$"
    )
    SOUS_TOTAL_RE = re.compile(
        r"Sous-total\s+Contrat\s+N°\s*(\d+)", re.I
    )

    FR_MONTHS = {
        "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
    }

    def parse(self, text: str) -> tuple[list[ContractBlock], Optional[str]]:
        """Return (blocks, facture_date_iso)."""
        facture_date = self._parse_facture_date(text)

        # Slice the text between contract headers.
        matches = list(self.CONTRACT_HEADER_RE.finditer(text))
        blocks: list[ContractBlock] = []
        for i, m in enumerate(matches):
            start = m.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            block_text = text[start:end]
            blk = self._parse_block(block_text, facture_date)
            if blk is not None:
                blocks.append(blk)
        return blocks, facture_date

    # ---- internals ----

    def _parse_facture_date(self, text: str) -> Optional[str]:
        m = self.FACTURE_DATE_RE.search(text)
        if not m:
            return None
        d, month_fr, y = m.groups()
        month = self.FR_MONTHS.get(month_fr.lower())
        if not month:
            return None
        return f"{y}/{month:02d}/{int(d):02d}"

    def _parse_block(self, block_text: str, facture_date: Optional[str]) -> Optional[ContractBlock]:
        head_m = self.CONTRACT_HEADER_RE.search(block_text)
        if head_m is None:
            return None
        number = head_m.group("number")
        plate = _normalize_plate(head_m.group("plate"))
        # Skip degenerate header matches (the plate must have at least 4 chars)
        if len(plate) < 4:
            return None

        blk = ContractBlock(plate=plate, number=number, lessor=self.lessor,
                            facture_date=facture_date, raw_text=block_text)

        # Duration + max mileage
        dm = self.DURATION_RE.search(block_text)
        if dm:
            try:
                blk.duration_months = int(dm.group(1))
                blk.max_mileage = int(re.sub(r"\s+", "", dm.group(2)))
            except ValueError:
                pass

        # Restitution date
        rm = self.RESTIT_RE.search(block_text)
        if rm:
            blk.restitution_date = _fr_to_iso(rm.group(1))

        # Period (start_date, end_date of the invoiced period)
        pm = self.PERIOD_RE.search(block_text)
        if pm:
            blk.start_date = _fr_to_iso(pm.group(1))
            blk.end_date = _fr_to_iso(pm.group(2))

        # Rubriques — walk lines, stop at Sous-total of this contract.
        # Exclude the header line that contains the contract number itself.
        for raw_line in block_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if self.SOUS_TOTAL_RE.search(line):
                break
            # Skip lines that are the contract header or obvious non-rubriques
            if self.CONTRACT_HEADER_RE.search(line):
                continue
            if line.lower().startswith("du ") and self.PERIOD_RE.search(line):
                continue
            tail = self.RUBRIQUE_TAIL_RE.search(line)
            if tail is None:
                continue
            ht = _to_float(tail.group("ht"))
            ttc = _to_float(tail.group("ttc"))
            # Label = everything before the first price on the line.
            prefix = line[: tail.start()].strip()
            # On Arval, the rubrique line format is: label prixU qty HT taux tva TTC.
            # The 'label prixU qty' sequence has 2 numbers. We strip trailing numbers from
            # the prefix to get just the label.
            label = re.sub(r"\s*" + _PRICE + r"(\s+" + _PRICE + r")?\s*$", "", prefix).strip()
            if not label:
                continue
            blk.rubriques.append(Rubrique(label=label, ht=ht, ttc=ttc))

        return blk


# ---------- Ayvens / Autre loueur — stubs ----------


class AyvensFactureParser(ArvalFactureParser):
    """Ayvens facture — same general shape as Arval (loyer + rubriques
    per contract in an 'Annexe'). Ayvens variants TBD when we get a
    sample; for now we reuse the Arval regex which has matched well on
    similar French lessor layouts. Overrideable field-by-field."""
    lessor = "ayvens"


class AutreLoueurFactureParser(ArvalFactureParser):
    """Fallback parser for loueurs we don't yet have a sample for.
    Reuses the generic pattern. Will be specialized per loueur as
    samples arrive."""
    lessor = "autre_loueur"


PARSERS: dict[str, type[ArvalFactureParser]] = {
    "arval": ArvalFactureParser,
    "ayvens": AyvensFactureParser,
    "autre_loueur": AutreLoueurFactureParser,
}


def detect_lessor(text: str) -> str:
    """Heuristic detection when no hint is provided."""
    lower = text.lower()
    if "arval" in lower:
        return "arval"
    if "ayvens" in lower or "ald automotive" in lower:
        return "ayvens"
    return "autre_loueur"


# ---------- Rubriques classifier ----------


def classify_rubriques(
    rubriques: list[Rubrique],
    whitelist: list[dict],
    blacklist: list[dict],
) -> None:
    """Tag each rubrique in place with its revio_field / blacklisted /
    unknown status based on the whitelist and blacklist regex lists.

    Match order: blacklist wins over whitelist (conservative: when in
    doubt, exclude). Within whitelist, the FIRST matching pattern wins.
    """
    compiled_white = [(re.compile(w["pattern"], re.I), w) for w in whitelist]
    compiled_black = [(re.compile(b["pattern"], re.I), b) for b in blacklist]
    for r in rubriques:
        label = (r.label or "").strip()
        if not label:
            r.unknown = True
            continue
        # Blacklist first
        if any(rx.search(label) for rx, _ in compiled_black):
            r.blacklisted = True
            continue
        # Whitelist
        hit = next(((rx, meta) for rx, meta in compiled_white if rx.search(label)), None)
        if hit is None:
            r.unknown = True
            continue
        _, meta = hit
        r.revio_field = meta.get("revio_field")
        r.contributes_to_total_only = bool(meta.get("contributes_to_total_only"))


# ---------- DataFrame assembly ----------


PRICE_FIELDS = [
    "civilLiabilityPrice", "allRisksPrice", "theftFireAndGlassPrice",
    "financialLossPrice", "legalProtectionPrice", "maintenancePrice",
    "replacementVehiclePrice", "tiresPrice", "gasCardPrice", "tollCardPrice",
]


def block_to_row(blk: ContractBlock, use_ttc: bool) -> dict[str, Any]:
    """Aggregate a ContractBlock into a flat dict row.

    `use_ttc=True` picks TTC amounts, else HT. The choice is driven by
    the contract's `isHT` (resolved by the rules engine) — we expose
    both at extraction time and let the engine pick.
    """
    row: dict[str, Any] = {
        "plate": blk.plate,
        "number": blk.number,
        "lessor": blk.lessor,
        "facture_date": blk.facture_date,
        "durationMonths": blk.duration_months,
        "maxMileage": blk.max_mileage,
        "restitutionDate": blk.restitution_date,
        "startDate": blk.start_date,
        "endDate": blk.end_date,
    }
    # totalPrice = Σ whitelist rubriques (both flavours, engine picks)
    total_ht = 0.0
    total_ttc = 0.0
    had_total_contribution = False
    for f in PRICE_FIELDS:
        row[f + "_ht"] = None
        row[f + "_ttc"] = None

    unknown: list[str] = []
    for r in blk.rubriques:
        if r.blacklisted:
            continue
        if r.unknown:
            unknown.append(r.label)
            continue
        if r.ht is not None:
            total_ht += r.ht
        if r.ttc is not None:
            total_ttc += r.ttc
        had_total_contribution = True
        if r.revio_field:
            key_ht = r.revio_field + "_ht"
            key_ttc = r.revio_field + "_ttc"
            # If multiple rubriques match the same field (rare), sum them.
            row[key_ht] = (row.get(key_ht) or 0.0) + (r.ht or 0.0)
            row[key_ttc] = (row.get(key_ttc) or 0.0) + (r.ttc or 0.0)

    row["totalPrice_ht"] = round(total_ht, 2) if had_total_contribution else None
    row["totalPrice_ttc"] = round(total_ttc, 2) if had_total_contribution else None
    row["_unknown_rubriques"] = unknown
    # Expose the engine-facing totalPrice now using the user-selected
    # flavour; engine can override if isHT resolution later differs.
    if use_ttc:
        for f in PRICE_FIELDS + ["totalPrice"]:
            row[f] = row.get(f + "_ttc")
    else:
        for f in PRICE_FIELDS + ["totalPrice"]:
            row[f] = row.get(f + "_ht")
    return row


def parse_factures_to_dataframe(
    pdf_paths: Iterable[str | Path],
    whitelist: list[dict],
    blacklist: list[dict],
    lessor_hint: Optional[str] = None,
    assume_ttc: bool = True,
) -> pd.DataFrame:
    """Parse one or more facture PDFs into a DataFrame indexed by (plate, number).

    Multiple factures covering the same (plate, number) are deduplicated
    — we keep the row coming from the most recent facture_date (R4).

    `assume_ttc` drives the initial totalPrice/prices flavour. The final
    HT/TTC choice is still the rules engine's call based on isHT.
    """
    all_rows: list[dict[str, Any]] = []
    for path in pdf_paths:
        text = _extract_text(path)
        lessor = lessor_hint or detect_lessor(text)
        parser_cls = PARSERS.get(lessor, AutreLoueurFactureParser)
        parser = parser_cls()
        blocks, facture_date = parser.parse(text)
        for blk in blocks:
            if blk.facture_date is None:
                blk.facture_date = facture_date
            classify_rubriques(blk.rubriques, whitelist, blacklist)
            row = block_to_row(blk, use_ttc=assume_ttc)
            row["_source_pdf"] = str(path)
            all_rows.append(row)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    # Dedup: keep most recent facture_date per (plate, number)
    df = df.sort_values(by=["plate", "number", "facture_date"],
                        ascending=[True, True, False],
                        na_position="last")
    df = df.drop_duplicates(subset=["plate", "number"], keep="first")
    df = df.reset_index(drop=True)
    return df


# ---------- Convenience loaders ----------


def load_rubriques_yml(path: str | Path) -> tuple[list[dict], list[dict]]:
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("whitelist", []), data.get("blacklist", [])
