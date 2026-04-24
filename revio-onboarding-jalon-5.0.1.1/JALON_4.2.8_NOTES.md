# Jalon 4.2.8 — Hotfix post-4.2.7 (2 bugs Augustin)

**Date** : 2026-04-24
**Scope** : `app.py`, `src/contract_engine.py`, `src/github_sync.py`, `src/rules/learned_columns.yml` (nouveau)
**Contexte** : post-4.2.7, Augustin a rechargé l'app et testé. Deux nouveaux
symptômes sur l'onglet Contract :

> **Screenshot 1** (clic 💾 Mémoriser ce format, fichier AEN Ayvens) :
> `0 champ(s) mémorisé(s), 2 échec(s) : plate: 'list' object has no attribute 'setdefault';
>  vehicleValue: 'list' object has no attribute 'setdefault'`
>
> **Screenshot 2** (clic 🤖 Proposer un mapping IA sur le client_file Contract) :
> `Erreur probe Contract : unsupported operand type(s) for -: 'str' and 'str'`

Les deux sont corrigés ici. Aucune refonte — hotfix pur.

---

## Bug A — `setdefault` crash au clic Mémoriser (retour du spectre de 4.2.7)

**Symptôme** : même erreur que 4.2.7 Bug A, mais sur un code path différent.
En 4.2.7 on avait normalisé `load_learned_patterns` pour gérer un YAML avec
`patterns: []`. Ça fonctionnait — en lecture. Mais à l'écriture, on réécrivait
le fichier avec `patterns: {dict}` → ça clobberait les signatures Vehicle
stockées dans le MÊME fichier `src/rules/learned_patterns.yml`.

**Cause racine** : deux systèmes de persistance partageaient le même fichier
avec des shapes incompatibles :
- `src/learned_patterns.py` (Vehicle, détection de format) : `patterns: [list of dicts]`
- `src/unknown_columns.py` (Contract, column overrides) : `patterns: {dict by table}`

Chaque module lisait en ignorant/normalisant l'autre, puis réécrivait dans son
propre format → perte de données à l'aller-retour. Le crash `setdefault`
était le symptôme d'un `patterns` qui n'avait pas été normalisé au bon
moment.

**Fix** : isolation complète. Le Contract écrit désormais dans un fichier
**distinct** :

| Fichier                                   | Owner                     | Shape          |
|-------------------------------------------|---------------------------|----------------|
| `src/rules/learned_patterns.yml`          | Vehicle (format signatures) | `patterns: []` (liste) |
| `src/rules/learned_columns.yml` *(nouveau)* | Contract (column overrides) | `patterns: {}` (dict)  |

```python
# app.py :: _render_contract_unknown_columns_ui
patterns_path = Path(__file__).parent / "src" / "rules" / "learned_columns.yml"
```

Nouveau helper GitHub dédié (symétrique de `save_learned_patterns_yaml`) :

```python
# src/github_sync.py
def save_learned_columns_yaml(yaml_text, *, commit_message, author_email):
    cfg = get_config()
    path = "src/rules/learned_columns.yml"  # hardcoded distinct path
    remote = fetch_file(cfg, path)
    ...
    return commit_file_at(cfg, path, yaml_text, sha=remote.sha, ...)
```

Fichier nouveau `src/rules/learned_columns.yml` créé avec une coquille
propre :

```yaml
# Learned column overrides — Contract side.
# (see header comment for format details)
patterns: {}
```

### Test

```python
>>> p = Path('/tmp/lc.yml'); p.write_text('patterns: {}\n')
>>> unkcol.register_learned_column(p, table='contract', source_slug='ayvens_aen',
...     field_name='plate', column='Immatriculation',
...     source_df=df, learned_by='augustin@gorevio.co')
>>> unkcol.register_learned_column(p, table='contract', source_slug='ayvens_aen',
...     field_name='vehicleValue', column='Prix achat net TTC', ...)
>>> print(p.read_text())
patterns:
  contract:
    ayvens_aen:
      plate:
        column: Immatriculation
        learned_on: '2026-04-24T15:07:41+00:00'
        learned_by: augustin@gorevio.co
        sample_values: [AB-123-CD]
      vehicleValue:
        column: Prix achat net TTC
        ...
```

Pas de crash. Plusieurs slugs et plusieurs fields cohabitent dans le YAML
résultant sans toucher à `learned_patterns.yml`.

---

## Bug B — `unsupported operand type(s) for -: 'str' and 'str'` au probe Contract

**Symptôme** : l'onglet Contract affichait en permanence
`Erreur probe Contract : unsupported operand type(s) for -: 'str' and 'str'`.
Le probe tourne à chaque render — le message restait donc affiché en
permanence, bloquant la visibilité des mappings et de la bannière doublons.

**Cause** : `_within_tolerance(a, b)` dans `src/contract_engine.py`
exécutait `abs(a - b)` sans vérifier que `a` et `b` soient numériques. La
fonction est appelée 2 fois dans `_resolve_cell` :

1. Ligne 389 — dans le bloc `if is_price` : **déjà gardée** par
   `isinstance(a, (int, float)) and isinstance(b, (int, float))`.
2. Ligne 409 — dans la construction de la raison de conflit pour le lineage :
   **pas gardée**.

Avec le nouveau flow IA de 4.2.7 où l'utilisateur peut mapper n'importe
quelle colonne client_file vers n'importe quel champ contractuel, le moteur
pouvait recevoir côté client_file un prix en string (ex: `"25000"`) et côté
loueur le même prix en float (`25000.0`). Les deux sont différentes au sens
`_values_differ` → on entre dans la construction du lineage → appel
`_within_tolerance("25000", 25000.0)` → `abs("25000" - 25000.0)` → crash.

**Fix** : guard isinstance dans `_within_tolerance` lui-même. Plus défensif
et robuste que guarder chaque call site.

```python
# src/contract_engine.py
def _within_tolerance(a: Any, b: Any, pct: float = 0.02, abs_tol: float = 2.0) -> bool:
    if a is None or b is None:
        return False
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return False  # (Jalon 4.2.8) inputs non-numeric → fall through to normal conflict path
    delta = abs(a - b)
    max_val = max(abs(a), abs(b))
    return delta <= max(abs_tol, pct * max_val)
```

### Tests

```
_within_tolerance('25000', '25000')     → False    (ex-crash, maintenant safe)
_within_tolerance(25000, 25000.1)       → True
_within_tolerance(25000, '25000')       → False
_within_tolerance(None, 25000)          → False
_within_tolerance(25000, 30000)         → False
_within_tolerance(25000.0, 25001.5)     → True
```

### E2E synthétique du scénario Augustin

```
client_df: GM234LM, GM555BB / Loyer="25000","18000" (strings)
ayvens_df: même plaques / Loyer périodique=25000.0, 18000.0 (floats)
overrides: client_file.plate ← Immat ; client_file.totalPrice ← Loyer

→ run_contract() ne crashe plus
→ totalPrice=25000.0 / 18000.0 (Ayvens gagne sur priorité)
→ 0 issues
```

---

## Fichiers modifiés

- `app.py`
  - `_render_contract_unknown_columns_ui` : `patterns_path` pointe sur
    `learned_columns.yml` (nouveau) au lieu de `learned_patterns.yml`
  - Commit GitHub du Mémoriser Contract utilise `save_learned_columns_yaml`
- `src/contract_engine.py`
  - `_within_tolerance` : guard isinstance défensif (les deux inputs doivent
    être numériques pour que la fonction calcule ; sinon `False`)
- `src/github_sync.py`
  - Ajout de `save_learned_columns_yaml` (symétrique de `save_learned_patterns_yaml`)
    qui commit sur `src/rules/learned_columns.yml`
- `src/rules/learned_columns.yml` ***(nouveau)*** — squelette `patterns: {}`
  avec commentaire d'entête décrivant le format.

Pas de changement : `src/unknown_columns.py` (déjà correct depuis 4.2.7),
`src/rules/contract.yml`, `src/rules/learned_patterns.yml` (Vehicle,
intouché — il garde sa shape `patterns: []`), `src/transforms.py`.

---

## À tester après déploiement

1. Onglet Contract, uploader 1 fichier loueur reconnu (ex: AEN Ayvens).
   Choisir un mapping manuel (ou cliquer Proposer IA) → cliquer
   **💾 Mémoriser ce format** → la bannière doit devenir verte :
   `✓ N champ(s) mémorisé(s) pour ce format.`
2. Aucun message d'erreur `'list' object has no attribute 'setdefault'`.
3. Vérifier dans GitHub que `src/rules/learned_columns.yml` est commité
   avec le slug + le mapping, **et** que `src/rules/learned_patterns.yml`
   reste inchangé (avec son `patterns: []`).
4. Onglet Contract avec client_file IA-mappé sur totalPrice → plus de
   `unsupported operand type(s) for -: 'str' and 'str'` en haut de page.
5. Cliquer Appliquer → export .zip → `contracts.csv` doit rester propre
   (maintenanceEnabled TRUE/FALSE, maintenanceNetwork any/specialist, dates
   YYYY/MM/DD).
