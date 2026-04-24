# Jalon 4.2.6 — Refactor Contract engine : plate-primary + parité Vehicle

**Date** : 2026-04-24
**Scope** : `src/contract_engine.py`, `app.py`
**Contexte** : post-4.2.5, Augustin a testé et signalé 4 blockers :
> 1. Matching par champ sans regrouper les fichiers — ça prend de la place.
> 2. Tu me demandes de matcher des champs déjà matchés dans la base véhicule (ex : immatriculation).
> 3. Le rendu final de l'export — j'ai rien du tout dans les contrats.
> 4. Règle simple : l'input c'est la plaque. Si on trouve les infos associées, on les remplit ; sinon on laisse vide. Même si obligatoire.

Cette release adresse les 4 points via une refonte de l'indexation du moteur Contract.

---

## 1. Plate-primary indexing (fix pour #3 et #4)

**Avant** (Jalon ≤4.2.5) : chaque source était indexée par une clé composite
`(plaque, numéro_contrat)`. La détection du numéro reposait sur une heuristique
(`_find_column` sur "contrat", "n°", "numéro", "réf"…) qui tombait sur des
colonnes piègeuses — typiquement `Réf. cli/cond.` du AND Ayvens, rempli avec
le NOM DU CONDUCTEUR (`DUFAU MARIE`, `RESTITUEE RESTITUEE`, etc.). Résultat :
les plaques ne matchaient plus entre sources (la clé `(GM234LM, TW90322)` côté
Ayvens ≠ `(GM234LM, None)` côté client_file), et l'export finissait vide ou
rempli de faux numéros.

**Après (4.2.6)** : **la plaque est la seule clé**. Plus d'indexation composite.
`number` redevient un simple champ tiré des règles YAML comme les autres.

```python
# src/contract_engine.py
def _make_key(plate, number=None, plate_only=True):
    p = plate_for_matching(plate) if plate is not None else None
    return p or None   # la clé EST la plaque, point.

def _index_by_plate(df):
    ...
    out["__key__"] = [_make_key(p) for p in out[plate_col]]
    out = out.drop_duplicates(subset=["__key__"], keep="first")
    return out.set_index("__key__", drop=True)
```

**Conséquences immédiates** :

| Before 4.2.6 | After 4.2.6 |
|---|---|
| Détection automatique du `number` nécessaire | Plus besoin — `number` suit les règles YAML |
| Clé composite → asymétrie entre sources → rows vides | 1 plaque → 1 ligne, enrichie avec ce qui est trouvé |
| `Réf. cli/cond.` → fausse colonne "numéro" → garbage | AND Ayvens passe par `N° Contrat` via la règle |
| `plate_only_mode` warning (signal de panne) | Mode normal : la plaque est toujours la clé |

**Règle non-blocking** : le moteur n'exige plus `number`, `durationMonths` ou
quoi que ce soit d'autre. Si la source ne l'a pas, la cellule reste vide, point.

## 2. Parité avec les mappings Vehicle (fix pour #2)

Augustin ne doit pas redemander à mapper `Immat` côté Contract alors qu'il
vient de le faire côté Vehicle. Le moteur Contract honore désormais les
`per_file_overrides` issus du session state partagé.

```python
# app.py — _build_contract_source_dfs
per_file_overrides = {
    scope: {field: col, ...}
    for (scope, field), col in st.session_state.engine_overrides.items()
    if scope in engine_files
}
merged = merge_engine_sources(engine_files, per_file_overrides=per_file_overrides)
```

Et côté engine, `_find_plate_column` lit en priorité les colonnes canoniques
émises par `merge_engine_sources` :

```python
for canonical in ("__map__plate", "__map__registrationPlate"):
    if canonical in df.columns:
        return canonical
```

→ Si l'utilisateur a mappé `registrationPlate ← Immat` dans l'onglet Véhicules,
Contract pose 0 question sur la plaque du client file.

## 3. UI mapping groupée par fichier (fix pour #1)

Refonte de `_render_contract_unknown_columns_ui(app.py)` : 1 expander par
FICHIER uploadé, listant tous les champs que le moteur n'a pas pu remplir
automatiquement depuis ce fichier.

**Avant** : 1 carte par champ × source → 12 cartes pour 4 champs × 3 fichiers.
**Après** : 1 expander par fichier (3 expanders), chaque expander liste les 4
champs avec un selectbox colonne source + bouton Mémoriser.

Les slugs pseudo (`backoffice`, `ep_loueur`, `api_plaque`, `derived`,
`rule_engine`, `*`) ne génèrent plus de cartes de mapping (pas de fichier à
pointer) — ils apparaissent dans un bloc info séparé "N champs non mappables
depuis un fichier" pour que l'utilisateur comprenne pourquoi `partnerId`
(backoffice) ou `isHT` (api_plaque) peuvent rester vides.

## 4. Avertissements pré-run

Le probe silencieux du moteur remonte maintenant :
- **Doublons de plaque** dans un fichier loueur — message `"N plaque(s) avec
  plusieurs lignes dans <slug> — seule la 1re ligne est retenue"` (gardé depuis
  4.2.5, mais désormais émis avec `source=<slug>` plutôt qu'un pseudo-flag).
- **Import sans fichier client** — message `"Import Contract sans fichier
  client — parc contrats dérivé de l'union des fichiers loueurs"`.

Ces warnings apparaissent en haut de l'onglet Contract, avant le bouton
Appliquer, comme sur Véhicules.

---

## Tests E2E validés (fichiers réels Augustin)

Sur les fichiers d'import du test du 2026-04-24 (client file 54 plaques,
Ayvens Etat de parc 33 lignes, AEN 2, AND 6, TVS 6, sans mapping contract-tab
mais avec mapping Vehicle `registrationPlate ← Immat`) :

```
=== Output ===
main df: 54 rows, 45 cols
orphan df: 4 rows (plaques présentes chez Ayvens, absentes du client file)
issues: 8 (cross-checks, pas de crash)

=== Fill rate ===
  plate: 54/54 ✓
  plateCountry: 54/54 ✓ (const_FR)
  number: 33/54 (les 33 matchés avec Ayvens Etat de parc)
  durationMonths: 33/54
  startDate: 33/54
  endDate: 33/54
  contractedMileage: 33/54
  totalPrice: 33/54

=== contracts.csv ===
54 lignes, 54 non-blanches. Exemple :
GM234LM,FR,,TW90322,,40,2023/03/27,27/07/2026,170000,180000,…
GM177QA,FR,,,,,,,,                  ← client-only, blanks normaux
```

→ Règle #4 respectée : client-only = plaque + FR, le reste vide.
→ Plus aucune chaîne comme `DUFAU MARIE` ou `PARC AIX` parasitant la colonne
  number — la détection composite a disparu.

---

## Fichiers modifiés

- `src/contract_engine.py`
  - docstring réécrite (plate-primary, Jalon 4.2.6)
  - `_make_key` simplifié (plate-only)
  - `_find_plate_column` : lit d'abord `__map__plate` / `__map__registrationPlate`
  - `_index_by_plate` remplace `_index_by_composite` (alias gardé pour back-compat)
  - `plate_only_mode` et sa heuristique supprimés
  - Warning doublons de plaque via Counter
  - Plate backfill simplifié (la clé EST la plaque)
  - Number backfill depuis la clé supprimé (plus de clé composite)
- `app.py`
  - `_build_contract_source_dfs` utilise `merge_engine_sources` avec les
    `per_file_overrides` du session state (partagé avec Vehicle)
  - `_render_contract_unknown_columns_ui` groupé par fichier (parité Vehicle)
  - Section "champs non mappables depuis un fichier" pour sources pseudo
  - Bannière warnings pré-run mise à jour

Pas de changement : `src/rules_engine.py`, `src/zip_writer.py`,
`src/rules/contract.yml`, `src/pipeline.py`.

---

## À tester après déploiement

1. Uploader un client file + 1 ou plusieurs fichiers loueurs, **sans** mapper
   quoi que ce soit côté Contract.
2. Vérifier qu'il n'y a **plus** de cartes de mapping demandant les mêmes
   champs qu'en Véhicules.
3. Lancer Appliquer → vérifier que l'export contient 1 ligne par plaque du
   client file, avec les données loueur là où la plaque matche.
4. Si un fichier loueur a plusieurs lignes pour la même plaque → vérifier que
   le warning "N plaques avec plusieurs lignes dans <slug>" apparaît en haut.
