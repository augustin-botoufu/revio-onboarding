# Jalon 4.2.9 — Hotfix Excel conflits + explications isHT & maintenanceEnabled

**Date** : 2026-04-24
**Scope** : `app.py`, `src/contract_engine.py`
**Contexte** : post-4.2.8, Augustin pose 3 questions :

> 1. *"Petit bug en jaune ?"* → bannière
>    `Rapport Excel contrats indisponible : 'tuple' object has no attribute 'get'`
> 2. *"Pourquoi on a jamais l'information is HT ? On avait défini des règles
>    ensemble à ce sujet. Tu t'en souviens ?"*
> 3. *"Peux tu me dire la règle mise en place pour dire true ou rien sur la
>    maintenance enabled ?"*

(1) et (2) sont corrigés ici. (3) est une question d'explication — la règle
existante est correcte, détaillée plus bas.

---

## Bug A — `'tuple' object has no attribute 'get'` au build du xlsx d'erreurs

**Symptôme** : bannière jaune au-dessus du bouton Télécharger zip du Contract :

```
Rapport Excel contrats indisponible : 'tuple' object has no attribute 'get'
```

Le zip se construit quand même, mais sans le `contracts_errors.xlsx`.

**Cause racine** : `src/contract_engine.py` stocke les conflits par cellule
comme une `list[tuple[source, value]]` (ligne 413) :

```python
conflicts.append((c["source"], c["value"]))
```

… mais `app.py::_build_contract_errors_xlsx` (ligne 2286) itérait dessus en
supposant des dicts :

```python
" | ".join(f"{c.get('source')}={c.get('value')} ({c.get('reason', '')})"
           for c in conflicts)
```

→ `tuple.get()` n'existe pas → la construction crashe, capturée par le
`except` plus haut qui affiche la bannière jaune.

**Fix** : unpack les tuples, tout en gardant la compat dict au cas où le
schéma évolue plus tard :

```python
for (key, field), conflicts in (result.conflicts_by_cell or {}).items():
    plate, _, number = str(key).partition("|")
    winner_src = (result.source_by_cell or {}).get((key, field))
    conflict_strs = []
    for c in conflicts or []:
        if isinstance(c, tuple) and len(c) >= 2:
            conflict_strs.append(f"{c[0]}={c[1]}")
        elif isinstance(c, dict):
            conflict_strs.append(
                f"{c.get('source')}={c.get('value')} ({c.get('reason', '')})"
            )
        else:
            conflict_strs.append(str(c))
    ws2.append([
        plate, number, field, str(winner_src),
        " | ".join(conflict_strs),
    ])
```

### Test (tuples uniquement, cas réel) :

```
>>> conflicts = [('ayvens_etat_parc', 25000.0), ('client_file', '25000')]
>>> " | ".join(...)
'ayvens_etat_parc=25000.0 | client_file=25000'
```

Plus de crash. La bannière jaune disparaît. Le `contracts_errors.xlsx`
repart avec l'onglet `conflits_cellule` peuplé normalement.

---

## Bug B — `isHT` toujours vide : détection VP trop stricte

**Symptôme** : dans l'output Contract, la colonne `isHT` est presque toujours
vide, même quand un fichier EP Ayvens a bien été uploadé avec l'info de genre
véhicule.

**Rappel de la règle qu'on avait définie ensemble** (cf. `src/rules/contract.yml`
lignes 152-174) :

```
isHT : Le contrat est-il exprimé Hors Taxes ?
  VP  → FALSE  (TTC — la TVA est incluse, contrat exprimé TTC)
  non-VP → TRUE (HT — la TVA est récupérable)

Priorité :
  1. VP lu dans le fichier EP loueur où le prix a été pris
  2. Fallback API Plaque (cf. table Vehicles, champ `usage`)
```

Les 2 `transform` déclarés dans le YAML (`rule_isHT_from_VP_EP`,
`rule_isHT_from_VP_API`) sont des **marqueurs** — c'est une post-passe
dédiée (`_postpass_isHT`, ligne 498 de contract_engine.py) qui résout
effectivement le champ.

**Cause racine** : dans `_extract_vp_from_ep_sources`, le matching de la
colonne « genre véhicule » ET des valeurs était trop strict :

- Headers acceptés : liste fermée `["genre", "type véhicule", "type vehicule",
  "catégorie véhicule", "vp", "vu", "vehicle_type"]` → rate "Genre du
  véhicule", "Type de véhicule", "Catégorie", "Classification VP/VU", etc.
- Valeurs acceptées : **égalité stricte**. Notamment
  `"VOITURE PARTICULIERE"` (sans accent) matchait mais
  `"VOITURE PARTICULIÈRE"` (avec accent, comme dans les vrais fichiers
  Ayvens) → **pas de match**.

Résultat : en pratique, aucun fichier EP ne passait la détection VP → la
post-passe retombait sur `vehicle_vp_by_plate`, qui est lui aussi vide si
l'utilisateur n'a pas cliqué Appliquer sur l'onglet Véhicules **avant**
l'onglet Contrats. → `isHT` restait None pour la quasi-totalité des lignes.

**Fix** : detection fuzzy par substring, à la fois sur les headers ET sur
les valeurs :

```python
VP_HEADER_HINTS = (
    "genre", "type véhicule", "type vehicule", "catégorie", "categorie",
    "vp/vu", "vp / vu", "vehicle_type", "type de véhicule",
    "type de vehicule", "classification", "nature du véhicule",
    "nature du vehicule",
)
VP_VALUE_HINTS = ("vp", "particul", "tourisme")
VU_VALUE_HINTS = ("vu", "utilit", "commercial", "pl", "camion", "fourgon")
```

### Tests

```
Genre du véhicule:
  "Voiture particulière" (avec accent)  → VP   ✓ (avant: empty)
  "VP"                                  → VP   ✓
  "Utilitaire léger"                    → VU   ✓ (avant: empty)
  "VU"                                  → VU   ✓
  "Camion"                              → VU   ✓ (avant: empty)
```

### Rappel UX

Si tu veux maximiser le taux de résolution `isHT` :

1. **Priorité 1** (EP loueur) — maintenant beaucoup plus tolérant, devrait
   attraper la plupart des Ayvens EP out-of-the-box.
2. **Priorité 2** (Vehicle result) — clique ▶️ Appliquer sur l'onglet
   **Véhicules** avant l'onglet Contrats. Le moteur Contrat lit alors
   `engine_result.df.usage` (private → VP, utility/service → non-VP).
   L'app affiche une caption `ℹ️ VP hérité du moteur Véhicules : N VP / M non-VP`
   quand c'est effectif.

---

## Question C — Règle `maintenanceEnabled` (TRUE / vide / FALSE)

*Augustin* : *"peux tu me dire la règle mise en place pour dire true ou rien
sur la maintenance enabled ?"*

**Règle définie dans `src/rules/contract.yml` (lignes 845-869)** :

```yaml
maintenanceEnabled:
  mandatory: false
  type: bool
  rules:
  - priority: 1
    source: ayvens_etat_parc
    column: Maintenance souscrite
    transform: rule_field_present
  - priority: 2
    source: autre_loueur_ep
    column: Maintenance souscrite
    transform: rule_field_present
  - priority: 3
    source: rule              # fallback si pas d'info EP
    transform: rule_price_positive
    column: maintenancePrice > 0
```

**Logique de `rule_field_present`** (cf. `src/transforms.py` lignes 175-184) :

```python
def rule_field_present(v):
    if _is_empty(v):
        return "FALSE"
    if _is_prestation_absent(str(v)):
        return "FALSE"
    return "TRUE"
```

Et `_is_prestation_absent` (lignes 147-172) :

```
Substrings qui forcent FALSE (ils doivent apparaître dans la valeur) :
  - "aucune prestation"
  - "aucun entretien"
  - "non souscrite", "non souscrit"
  - "pas de prestation", "pas de maintenance", "pas d'entretien"
  - "hors prestation"
  - "sans prestation", "sans maintenance"

Égalité stricte qui force FALSE (toute la cellule doit l'être) :
  - "aucun", "aucune", "non", "nc", "n/a", "na", "-", "/"
```

### Donc concrètement :

| Valeur dans « Maintenance souscrite »        | maintenanceEnabled |
|----------------------------------------------|--------------------|
| (vide)                                       | FALSE              |
| "nc", "n/a", "-"                             | FALSE              |
| "aucune prestation"                          | FALSE              |
| "non souscrite"                              | FALSE              |
| "Pas de maintenance"                         | FALSE              |
| "Réseau Ayvens"                              | **TRUE**           |
| "Autre prestation maintenance"               | **TRUE**           |
| "Constructeur"                               | **TRUE**           |
| n'importe quelle autre valeur non vide       | **TRUE**           |

### Pourquoi tu vois « TRUE ou rien » et jamais FALSE ?

La seule façon de récolter une valeur **vide** en sortie (= "rien") est que
le moteur n'ait trouvé **aucune** source contributrice :

1. Aucun fichier `ayvens_etat_parc` ou `autre_loueur_ep` uploadé, **ou**
2. Le fichier EP est là mais la colonne `Maintenance souscrite` a un nom
   différent ET n'a pas été mappée à la main (elle n'est pas `mandatory`,
   donc elle ne remonte pas dans la carte de mapping obligatoire),
3. Le `maintenancePrice` du fallback priorité 3 est lui aussi vide / 0.

Dans ces cas, le moteur ne peut rien décider → cellule vide, pas FALSE.

Par contre, si la colonne « Maintenance souscrite » existe dans l'EP et
qu'une cellule est vide pour une ligne donnée, la règle retourne bien FALSE
pour cette ligne (via `_is_empty` → return "FALSE").

### Recommandation

Si tu veux systématiquement une valeur TRUE / FALSE (jamais vide), 2
options :

- **A. Déclarer `maintenanceEnabled` mandatory** → le moteur te ferait
  mapper la colonne s'il ne la reconnaît pas, et tous les loueurs
  produiraient FALSE quand rien n'est dispo.
- **B. Garder `mandatory: false`** mais ajouter une règle de priorité 4
  `source: *` avec une constante FALSE pour forcer un défaut.

Je propose qu'on en parle avant de choisir — dis-moi laquelle te va.

---

## Fichiers modifiés

- `app.py`
  - `_build_contract_errors_xlsx` (ligne 2286) : unpack des tuples
    `(source, value)` dans l'onglet `conflits_cellule`.
- `src/contract_engine.py`
  - `_extract_vp_from_ep_sources` (lignes 547-601) : detection fuzzy
    (substring) sur les headers VP/VU et sur les valeurs, avec support
    des accents français.

Pas de changement : contract.yml (règles inchangées — c'est du tuning
Python), transforms.py, learned_columns.yml, learned_patterns.yml, etc.

---

## À tester après déploiement

1. Relancer un run Contract avec un conflit sur un prix (ex: client_file
   en string + Ayvens AEN en float, comme en 4.2.8). Le zip doit contenir
   `contracts_errors.xlsx` avec un onglet `conflits_cellule` rempli
   (plus de bannière jaune).
2. Uploader un Ayvens EP avec colonne "Genre du véhicule" (valeurs type
   "Voiture particulière", "Utilitaire"). La colonne `isHT` dans le CSV
   contrats doit maintenant être peuplée (TRUE pour VU, FALSE pour VP).
3. Si toujours vide : cliquer ▶️ Appliquer sur l'onglet Véhicules
   **avant** de relancer Contrat, puis vérifier le message
   `ℹ️ VP hérité du moteur Véhicules : …`.
4. Vérifier qu'`maintenanceEnabled` est TRUE quand "Maintenance souscrite"
   est "Réseau Ayvens" et vide quand la colonne n'est pas dans le fichier.
