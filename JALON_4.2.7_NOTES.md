# Jalon 4.2.7 — Hotfix post-4.2.6 (4 bugs Augustin)

**Date** : 2026-04-24
**Scope** : `src/transforms.py`, `src/contract_engine.py`, `src/unknown_columns.py`, `app.py`
**Contexte** : post-4.2.6, Augustin a testé et remonté 4 bugs :

> 1. Pourquoi pour les champs à mapper on ne fonctionne pas comme pour véhicule — le fichier/table, tous les champs, la possibilité d'utiliser l'IA et de mémoriser le mapping ? Là on a un bouton Mémoriser par champ.
> 2. Pourquoi j'indique les 4 immat manquantes directement en haut alors que j'en reparle en bas ?
> 3. Dans le fichier output, `maintenanceEnabled` les valeurs c'est `TRUE` ou `FALSE`. Là j'ai `autre prestation maintenance`. Idem `maintenanceNetwork`. Les dates ne semblent pas au bon format (il y a une heure).
> 4. `'list' object has no attribute 'setdefault'` quand je clique Mémoriser.

Les 4 sont corrigés ici. La release est un hotfix — aucun changement de logique moteur, uniquement des fixes de registre de transforms, d'UX et de parsing.

---

## Bug A (#4) — `setdefault` crash au clic Mémoriser

**Symptôme** : `AttributeError: 'list' object has no attribute 'setdefault'` dans
`register_learned_column` au moment où l'utilisateur enregistrait un mapping.

**Cause** : d'anciennes versions de `learned_patterns.yml` checkées en repo ont
`patterns: []` (liste vide) au lieu de `patterns: {}`. `setdefault(table, {})`
crashe sur une liste.

**Fix** : `load_learned_patterns` normalise maintenant `data["patterns"]` en
dict même si le YAML présente une liste ou autre type :

```python
# src/unknown_columns.py
if not isinstance(data.get("patterns"), dict):
    data["patterns"] = {}
```

---

## Bug B (#3b) — Dates au format `2023-03-27 00:00:00`

**Symptôme** : l'export `contracts.csv` affichait `2023-03-27 00:00:00` (repr
pandas Timestamp) au lieu de `2023/03/27`.

**Cause** : les règles `contract.yml` utilisent des noms de transforms
spécifiques (`date_iso`, `date_fr_to_iso`, `date_any_to_iso`, `parse_date_fr`)
qui n'étaient pas enregistrés dans `TRANSFORMS`. Le moteur retombait sur un
passthrough silencieux et le Timestamp fuyait tel quel.

**Fix** : `src/transforms.py` enregistre désormais chaque alias, tous mappés
vers `normalize_date` (qui gère déjà les `pd.Timestamp`, `datetime`, ISO,
DD/MM/YYYY, YYYY-MM-DD, etc. → sortie canonique `YYYY/MM/DD`) :

```python
parse_date_fr  = normalize_date
date_iso       = normalize_date
date_any_to_iso = normalize_date
date_fr_to_iso = normalize_date

TRANSFORMS = {
    ...
    "parse_date_fr":   parse_date_fr,
    "date_iso":        date_iso,
    "date_any_to_iso": date_any_to_iso,
    "date_fr_to_iso":  date_fr_to_iso,
    ...
}
```

Vérifié :
```
date_iso('2023-03-27 00:00:00')                → '2023/03/27'
date_iso(pd.Timestamp('2023-03-27'))           → '2023/03/27'
date_iso('27/03/2023')                         → '2023/03/27'
```

---

## Bug C (#3a) — `maintenanceEnabled` / `maintenanceNetwork` contiennent du texte brut

**Symptôme** : `maintenanceEnabled` recevait `"autre prestation maintenance"` au
lieu de `TRUE`/`FALSE`. Idem `maintenanceNetwork` (devait être `any` /
`specialist`).

**Cause** : `rule_field_present` et `enum_mapping_2` étaient référencés dans
`contract.yml` mais absents du registre `TRANSFORMS`. Conséquence : le moteur
poussait la valeur brute dans la cellule en émettant un warning "Transform
inconnu".

**Fix** : les deux transforms sont implémentés et enregistrés :

```python
# bool présent / absent — utilisé pour *Enabled
_PRESTATION_ABSENT_SUBSTRINGS = (
    "aucune prestation", "aucun entretien",
    "non souscrite", "non souscrit",
    "pas de prestation", "pas de maintenance", "pas d'entretien",
    "hors prestation", "sans prestation", "sans maintenance",
)
_PRESTATION_ABSENT_EXACT = {"aucun", "aucune", "non", "nc", "n/a", "na", "-", "/"}

def rule_field_present(v):
    if _is_empty(v):          return "FALSE", []
    if _is_prestation_absent(str(v)): return "FALSE", []
    return "TRUE", []

def enum_mapping_2(v):
    """any / specialist. 'réseau ayvens' / 'tous réseaux' / 'libre' → any.
    'autre prestation' / 'agréé' → specialist."""
    ...
```

### Piège résolu au passage : faux positif `"nc"`

Première itération du fix : `_PRESTATION_ABSENT_MARKERS` était une liste de
sous-chaînes incluant `"nc"`. Problème : `"nc" in "autre prestation maintenance"`
vaut `True` parce que **"mainte*nc*e"** contient la séquence. Résultat :
`rule_field_present('autre prestation maintenance')` retournait `FALSE`.

Correction : séparation en deux structures :
- `_PRESTATION_ABSENT_SUBSTRINGS` (phrases non-ambiguës) → testées en
  substring containment.
- `_PRESTATION_ABSENT_EXACT` (set de tokens courts type `"nc"`, `"n/a"`) →
  testées en égalité stricte après `strip().lower()`.

Vérifié :
```
rule_field_present('autre prestation maintenance') → 'TRUE'
rule_field_present('nc')                            → 'FALSE'
rule_field_present('NC')                            → 'FALSE'
rule_field_present('maintenance standard')          → 'TRUE'   (pas de faux positif)
rule_field_present('aucune prestation')             → 'FALSE'
enum_mapping_2('réseau ayvens')                     → 'any'
enum_mapping_2('autre prestation maintenance')      → 'specialist'
```

### Autres transforms ajoutés au registre dans la foulée

Même cause (référencé YAML ↔ absent du registre) :
- `strip`, `float_fr`, `float_fr_rubrique`, `bool_fr`, `int`
- `enum_mapping` (catégorie VR A–H), `enum_mapping_tires` (standard / winter / 4seasons)
- `rule_source_present` (retourne `TRUE` si non vide, sinon None — utilisé
  par replacementVehicleEnabled & co.)

### Passthrough markers côté engine

`rule_price_positive`, `rule_or`, `lookup_partner`, `lookup_by_source_slug`
sont des transforms *résolus ailleurs* (post-passes, enrichissement index).
Ajoutés à la liste des markers que `_apply_rule_transform` laisse passer sans
warning :

```python
# src/contract_engine.py ::_apply_rule_transform
if name in {"cross_check", "BANNED", "rule_isHT_from_VP_EP",
            "rule_isHT_from_VP_API", "compute_months",
            "sum_whitelist", "regex_number", "regex_duration",
            "regex_mileage", "regex_start_date", "regex_restit_date",
            "rule_price_positive", "rule_or",
            "lookup_partner", "lookup_by_source_slug"}:
    return raw, []
```

---

## Bug #2 — Bannière orphelines dupliquée en haut

**Symptôme** : les 4 plaques présentes chez Ayvens mais absentes du client
file apparaissaient **deux fois** : une pré-run en haut de l'onglet Contract
(issu du probe silencieux), une post-run en bas (tableau orphelines officiel).

**Fix** : on supprime la bannière pré-run `cross_by_slug` qui affichait les
plaques orphelines individuellement. L'utilisateur les verra au moment où ça
compte — dans la section dédiée après Appliquer. On garde en bannière
pré-run : doublons de plaque + absence de client_file (ces deux-là ne sont
pas reproduits plus bas).

---

## Bug #1 — UX mapping : parité totale avec Vehicle

**Avant (4.2.6)** : 1 expander par fichier **mais** un bouton Mémoriser par
champ. Pas de bouton IA. Pas de vue "tous les champs mappables", seulement
ceux flaggés `unknown`.

**Après (4.2.7)** : reprise *pixel-perfect* du schéma Véhicules :

1. **1 expander par fichier uploadé** (`1 fichier → 1 carte`).
2. Liste **tous les champs mappables** depuis ce fichier (parse YAML,
   inverse `rules.source == slug` pour obtenir field list par slug).
3. **Bouton 🤖 Proposer (IA)** qui appelle `propose_mapping(df, "contract")`
   et pré-remplit tous les selectbox.
4. **1 bouton global 💾 Mémoriser ce format** par fichier — enregistre
   toutes les paires field→column non vides dans `learned_patterns.yml`
   d'un coup (cohérent avec Vehicle).
5. **State partagé** : les mappings éditables tapent dans
   `st.session_state.engine_overrides[(file_key, field)]`, qui est la même
   structure que Vehicle. Conséquence (déjà en 4.2.6 mais désormais
   utilisée des deux côtés) : mapper `registrationPlate ← Immat` dans
   l'onglet Véhicules **neutralise** la carte plate côté Contract.
6. **Bloc info séparé** "N champs non mappables depuis un fichier" pour les
   slugs pseudo (`backoffice`, `ep_loueur`, `api_plaque`, `derived`,
   `rule_engine`, `*`) — l'utilisateur comprend pourquoi `partnerId`
   (backoffice) ou `isHT` (api_plaque) peuvent rester vides sans panique.

Nouveau helper :
```python
# app.py
_CONTRACT_PSEUDO_SLUGS = {"backoffice", "ep_loueur", "api_plaque",
                          "derived", "rule_engine", "rule", "*"}

@functools.lru_cache(maxsize=1)
def _contract_fields_by_slug() -> dict[str, list[str]]:
    """Parse contract.yml, invert rules → {slug: [field, ...]}."""
```

---

## Canonical overrides plus robustes

En refactorant `_build_contract_source_dfs` pour faire tourner la parité avec
Vehicle, on émet désormais explicitement les overrides canoniques à passer au
moteur Contract :

```python
# app.py
def _build_contract_source_dfs(engine_files):
    per_file_overrides = {}
    canonical_overrides = {}
    engine_file_keys = set(engine_files.keys())
    for (scope, field_name), src_col in st.session_state.get("engine_overrides", {}).items():
        if not src_col or scope not in engine_file_keys:
            continue
        per_file_overrides.setdefault(scope, {})[field_name] = src_col
        slug_for_file = engine_files[scope].get("slug")
        if slug_for_file:
            canonical_overrides[(slug_for_file, field_name)] = f"__map__{field_name}"
    merged = merge_engine_sources(engine_files, per_file_overrides=per_file_overrides)
    return {slug: ms.df for slug, ms in merged.items()}, canonical_overrides
```

Les deux call sites (probe silencieux + bouton Appliquer) fusionnent maintenant
`canonical_overrides` avec `contract_unknown_resolved` avant de passer à
`run_contract()`.

---

## Tests E2E synthétiques

```
$ python3 -c "… test harness inline …"

--- rule_field_present ---
  rule_field_present('autre prestation maintenance') -> 'TRUE'
  rule_field_present('réseau ayvens')                -> 'TRUE'
  rule_field_present('aucune prestation')            -> 'FALSE'
  rule_field_present('nc')                            -> 'FALSE'
  rule_field_present('NC')                            -> 'FALSE'
  rule_field_present('maintenance standard')          -> 'TRUE'
  rule_field_present('-')                             -> 'FALSE'
  rule_field_present(None)                            -> 'FALSE'

--- enum_mapping_2 ---
  enum_mapping_2('réseau ayvens')                     -> 'any'
  enum_mapping_2('autre prestation maintenance')      -> 'specialist'
  enum_mapping_2('garage agréé')                      -> 'any'

--- date_iso ---
  date_iso('2023-03-27 00:00:00')                     -> '2023/03/27'
  date_iso(Timestamp('2023-03-27 00:00:00'))          -> '2023/03/27'
  date_iso('27/03/2023')                              -> '2023/03/27'

--- bool_fr ---
  bool_fr('oui')/'non'/'OUI'/'1'/'0'                  -> 'TRUE'/'FALSE' attendus

--- run_contract E2E (5 plaques, ayvens_etat_parc 3 lignes) ---
    plate   plateCountry   number   durationMonths  startDate   endDate    totalPrice  maintenanceEnabled  maintenanceNetwork
0   GM-234-LM   FR          TW90322  40             2023/03/27  2026/07/27  25000       TRUE                any
1   GM-177-QA   FR          NaN      NaN            NaN         NaN         NaN         NaN                 NaN        ← client-only OK
2   GM-999-AA   FR          NaN      NaN            NaN         NaN         NaN         NaN                 NaN        ← client-only OK
3   GM-555-BB   FR          TW90400  36             2024/01/01  2027/01/01  18000       TRUE                specialist
4   GM-333-CC   FR          TW90500  36             2023/06/15  2026/06/15  15000       FALSE               NaN         ← "aucune prestation" → Enabled=FALSE

issues: 0
```

→ `maintenanceEnabled` propre (TRUE/FALSE), `maintenanceNetwork` propre
  (any/specialist), dates propres (YYYY/MM/DD, pas d'heure).
→ Règle #4 Jalon 4.2.6 respectée : client-only = plaque + FR, reste vide.
→ 0 issue sur ce cas nominal.

Compilation :
```
$ python3 -m py_compile app.py src/transforms.py src/contract_engine.py src/unknown_columns.py
COMPILE_OK
```

---

## Fichiers modifiés

- `src/transforms.py`
  - Aliases dates (`parse_date_fr`, `date_iso`, `date_any_to_iso`, `date_fr_to_iso`)
  - `strip`, `float_fr`, `float_fr_rubrique`, `bool_fr`, `int`
  - `_PRESTATION_ABSENT_SUBSTRINGS` + `_PRESTATION_ABSENT_EXACT` + `_is_prestation_absent`
  - `rule_field_present`, `rule_source_present`
  - `enum_mapping` (catégorie VR A-H), `enum_mapping_2` (any/specialist),
    `enum_mapping_tires` (standard/winter/4seasons)
  - `TRANSFORMS` complétée avec toutes ces entrées
- `src/contract_engine.py`
  - Passthrough markers étendus (`rule_price_positive`, `rule_or`,
    `lookup_partner`, `lookup_by_source_slug`) dans `_apply_rule_transform`
- `src/unknown_columns.py`
  - `load_learned_patterns` normalise `data["patterns"]` → dict quoi qu'il arrive
- `app.py`
  - `_build_contract_source_dfs` retourne `(source_dfs, canonical_overrides)`
  - Les deux callers (probe + Appliquer) fusionnent `canonical_overrides`
    avec `contract_unknown_resolved`
  - `_render_contract_unknown_columns_ui` réécrit complet : 1 expander /
    fichier, bouton IA, bouton Mémoriser global, tous les champs mappables
    listés
  - Helper `_contract_fields_by_slug` (parse YAML cached)
  - Constante `_CONTRACT_PSEUDO_SLUGS`
  - Bannière pré-run cross_by_slug retirée (fix Bug #2)

Pas de changement : `src/rules_engine.py`, `src/zip_writer.py`,
`src/rules/contract.yml`, `src/pipeline.py`, `src/llm_mapper.py`.

---

## À tester après déploiement

1. Uploader client_file + 2-3 fichiers loueurs connus (Ayvens EP, TVS, AEN).
2. Vérifier que `Mémoriser ce format` apparaît **une seule fois par fichier**,
   pas par champ.
3. Cliquer **Proposer (IA)** → vérifier que les champs se pré-remplissent.
4. Lancer Appliquer → ouvrir `contracts.csv` :
   - `maintenanceEnabled` ne contient que `TRUE`/`FALSE` (ou vide).
   - `maintenanceNetwork` ne contient que `any`/`specialist` (ou vide).
   - Les dates n'ont **pas** de composante heure : `2023/03/27`, pas
     `2023-03-27 00:00:00`.
5. Si le client file a moins de plaques que les fichiers loueurs → vérifier
   que les plaques orphelines n'apparaissent qu'en bas (section dédiée),
   plus en haut.
6. Si le client file a un mapping `registrationPlate ← Immat` côté Véhicules
   → vérifier qu'**aucune carte** ne redemande la plaque dans Contract.
