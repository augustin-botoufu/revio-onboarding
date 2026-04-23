# revio_engine_contract_v1 — Release backend Jalon 4.1

Moteur de règles Contract + side-car lineage partagé avec le moteur Vehicle.
Cette release est **backend-only** : l'intégration UI Streamlit (onglets
Moteur + Règles d'import multi-bases) est le Jalon 4.2 (prochaine session).

## Contenu

```
revio_engine_contract_v1/
├── README.md                    ← ce fichier
├── test_integration.py          ← test end-to-end (PDF Arval + client_file fake)
└── src/
    ├── __init__.py
    ├── lineage.py               ← NEW : LineageStore + LineageRecord (partagé V/C)
    ├── rules_engine.py          ← Vehicle, retrofitté lineage (non-breaking)
    ├── contract_engine.py       ← NEW : moteur Contract (clé composite plate|number)
    ├── pdf_parser.py            ← NEW : Arval/Ayvens/AutreLoueur → DataFrame
    ├── unknown_columns.py       ← NEW : flow colonne non identifiée
    ├── rules_io.py, transforms.py, normalizers.py   ← communs (inchangés)
    └── rules/
        ├── vehicle.yml          ← inchangé
        ├── contract.yml         ← NEW : 45 champs, 149 règles (généré depuis spec v2)
        ├── rubriques_facture.yml  ← NEW : 11 whitelist + 28 blacklist
        ├── partner_index.yml    ← NEW : 12 partenaires (loueurs, cartes, télébadge)
        └── learned_patterns.yml ← colonnes apprises (Vehicle + Contract)
```

## Les 5 briques livrées

1. **Lineage (`src/lineage.py`)**
   Provenance par cellule (table, key, field, value, source_used, priority,
   transform, rule_id, conflicts_ignored, notes, warnings). Side-car parquet
   (fallback jsonl si pyarrow absent). Alimente l'assistant LLM du Jalon 5.0.

2. **Retrofit Vehicle (`src/rules_engine.py`)**
   `EngineResult.lineage` est désormais peuplé pour chaque cellule Vehicle
   sans casser l'existant. `apply_rules(..., table="vehicle")` par défaut.

3. **YAML Contract (`rules/contract.yml` + co)**
   Généré par `/outputs/spec_contract/gen_yaml.py` depuis
   `spec_contract_v2.xlsx`. Clé primaire : `(plate, number)`. Rubriques
   classées whitelist/blacklist. Index partnerId (UUID loueurs) inclus.

4. **Parser PDF factures (`src/pdf_parser.py`)**
   Dispatch par loueur (Arval impl. full, Ayvens/AutreLoueur héritent —
   à spécialiser sur samples). Regex contrat, durée, période, date.
   `parse_factures_to_dataframe()` dé-duplique par `(plate, number)` en
   gardant la facture la plus récente (R4 de la spec).

5. **Moteur Contract (`src/contract_engine.py`)**
   - Clé composite `"{plate}|{number}"` (index `contract_key`).
   - `_resolve_cell` → priorité, tolérance (2 % + 2 €) sur prix, lineage.
   - Post-pass : `durationMonths` (endDate − startDate), `isHT` (VP de l'EP
     P1, fallback API_Plaque P2).
   - Cross-check plaques : lignes des EP loueurs absentes du `client_file`
     → `issues` (futur `contracts_errors.xlsx`).
   - `unknown_column_requests` : champs mandatory non résolvables → flow UI.

6. **Flow colonne non identifiée (`src/unknown_columns.py`)**
   Persiste dans `learned_patterns.yml` le choix utilisateur (`column`,
   `learned_on`, `learned_by`, `sample_values`). Consommable par les 2
   moteurs via `learned_patterns_to_overrides(path, table)`.

## Test d'intégration

```bash
cd revio_engine_contract_v1/
python test_integration.py
```

Fixture : PDF Arval réel (5 contrats extraits) + client_file minimal
(2 plaques matching). Résultat :
- 2 contrats populés, 3 orphans flaggés
- 26 lineage records
- issues = 3 (cross-check), unknown_column_requests = 8 (champs optionnels)

## Points d'attention pour le Jalon 4.2 (UI)

- La **résolution de la colonne `plate` depuis `client_file`** nécessite
  que le nom réel de colonne (`Immatriculation`, `N° immat`…) soit
  déclaré dans `contract.yml` OU soit injecté via
  `manual_column_overrides` au run-time. Sinon `plate`/`number` restent
  vides en sortie malgré une clé composite correcte.
- Les parsers Ayvens et AutreLoueur héritent d'Arval : à spécialiser dès
  qu'on récupère des samples réels.
- `parquet` requiert `pyarrow` → fallback `jsonl` transparent si absent.
  En prod Streamlit Cloud, ajouter `pyarrow` au `requirements.txt`.

## Commande de régénération des YAML

```bash
cd /outputs/spec_contract
python gen_yaml.py
# → rules/contract.yml, rules/rubriques_facture.yml, rules/partner_index.yml
```

À rejouer après chaque édition de `spec_contract_v2.xlsx`.

---

Jalon 4.2 (prochaine session) : onglets Vehicle/Contract sur page Moteur,
onglets Règles d'import par base, zip de sortie enrichi (contracts.xlsx +
contracts_errors.xlsx), segmentation flotte/agence appliquée aux contrats.

Jalon 5.0 (après) : assistant LLM in-app consommant les lineage sidecars.
