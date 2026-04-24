# Jalon 4.3.0 — Re-hydratation `learned_columns.yml` à l'upload

**Date** : 2026-04-24
**Scope** : `app.py`
**Contexte** : préparation du Jalon 4.3.1 (écran d'audit des mappings).
Plomberie invisible qui résout le bug de fond avant d'ajouter de l'UI
par-dessus.

---

## Le bug

Quand l'utilisateur clique **💾 Mémoriser ce format** dans l'onglet
Contrats, le mapping est persisté dans `src/rules/learned_columns.yml`
via `unkcol.register_learned_column(...)`. Vérifié côté disque : le YAML
grossit bien à chaque Mémoriser.

**Mais** — et c'est le trou — personne ne relit jamais ce fichier au
moment de l'upload. Résultat :

- À la prochaine session, le fichier est ré-uploadé.
- Le moteur Contract ne voit pas le mapping mémorisé.
- La carte « Colonne non identifiée » réapparaît, comme si rien n'avait
  été mémorisé.
- L'utilisateur re-clique Mémoriser, ça écrase la ligne précédente avec
  la même valeur, et boucle.

Les fonctions utilitaires `unkcol.learned_patterns_to_overrides(...)` et
`unkcol.suppress_resolved_requests(...)` existent **dans le code depuis
le Jalon 4.1.7** (octobre 2025) mais n'avaient jamais été branchées.
Asymétrie avec Véhicules, qui lui re-hydrate bien son `learned_patterns.yml`
via `lp.load_patterns()` à chaque upload (cf. app.py ligne 1751).

## Le fix

À l'ouverture du handler d'upload, on lit `learned_columns.yml` UNE
fois et on l'aplatit en `{slug: {field: source_col}}`. Puis pour chaque
fichier uploadé (PDF + tabulaire), après détection du slug, on applique
les mappings correspondants à `st.session_state.engine_overrides`.

```python
# app.py, à l'entrée du handler d'upload (après _patterns = lp.load_patterns())
_learned_cols_by_slug: dict[str, dict[str, str]] = {}
try:
    _lc_data = unkcol.load_learned_patterns(_learned_columns_path)
    for _table_name in ("vehicle", "contract"):
        _table_node = _lc_data.get("patterns", {}).get(_table_name, {}) or {}
        for _slug_key, _fields_node in _table_node.items():
            _slug_out = _learned_cols_by_slug.setdefault(_slug_key, {})
            for _field_name, _entry in (_fields_node or {}).items():
                _col = _entry.get("column") if isinstance(_entry, dict) else None
                if _col and _field_name not in _slug_out:
                    _slug_out[_field_name] = _col
except Exception:
    _learned_cols_by_slug = {}  # jamais bloquer un upload sur un YAML malformé
```

Et dans le loop par fichier :

```python
def _apply_learned_columns(key, slug, df) -> int:
    learned_cols = _learned_cols_by_slug.get(slug, {})
    if not learned_cols:
        return 0
    valid_cols = {str(c) for c in df.columns}
    applied = 0
    for field, src_col in learned_cols.items():
        if not src_col or src_col not in valid_cols:
            continue
        ov_key = (key, field)
        if ov_key in st.session_state.engine_overrides:
            continue  # Pattern match Vehicle a priorité (plus spécifique)
        st.session_state.engine_overrides[ov_key] = src_col
        applied += 1
    return applied
```

### Précédence choisie

1. **Pattern match Vehicle** (`learned_patterns.yml` — file-signature
   spécifique) pose ses `column_mapping` en premier.
2. **learned_columns.yml** (slug-wide) pose par-dessus, sans écraser ce
   qui est déjà là.

Rationale : un pattern qui a identifié un fichier par sa signature
(filename + columns) connaît précisément le mapping pour CE fichier. Le
mapping slug-wide est une règle générique pour tous les fichiers du
même slug — plus faible en information.

En pratique ces deux mécanismes ne se chevauchent quasi jamais sur les
mêmes champs (Vehicle stocke surtout des champs véhicule ; Contract
stocke des champs contrat), mais la règle existe au cas où.

### Feedback UX

Un nouveau bandeau `🗂️ Mappings mémorisés réappliqués` apparaît après
le succès de l'upload, distinct du bandeau `🧠 Format reconnu` existant.
Les deux mécanismes étant indépendants, les distinguer visuellement
aide à déboguer si jamais un des deux ne fait pas son job.

## Test du flatten logic

```python
>>> yaml_content = '''
... patterns:
...   contract:
...     ayvens_etat_parc:
...       number:
...         column: "N° Contrat"
...       startDate:
...         column: "Date début"
... '''
>>> data = yaml.safe_load(yaml_content)
>>> flatten(data)
{'ayvens_etat_parc': {'number': 'N° Contrat', 'startDate': 'Date début'}}
```

OK.

## Ce que ça change pour Augustin

**Session N** :
1. Tu uploades un Ayvens EP, le moteur te demande de mapper
   `batteryValue` (ou n'importe quel champ non reconnu).
2. Tu mappes, tu cliques Mémoriser.
3. Zip téléchargé. Tu fermes l'app.

**Session N+1** (avant 4.3.0) :
- Tu uploades le même type de fichier. La carte « Colonne non
  identifiée » revient. Tu remappes. Tu re-cliques Mémoriser. Frustrant.

**Session N+1** (avec 4.3.0) :
- Tu uploades le même type de fichier. Bandeau
  `🗂️ Mappings mémorisés réappliqués — ayvens_etat_parc.xlsx → 3 champ(s)`.
- La carte « Colonne non identifiée » **ne revient pas** pour ces champs.
- Tu cliques directement Appliquer. Fini.

## Fichiers modifiés

- `app.py`
  - Handler d'upload (ligne ~1751-1850) : pré-chargement de
    `learned_columns.yml`, helper interne `_apply_learned_columns`,
    application dans les 2 branches (PDF + tabulaire), bandeau
    `🗂️ Mappings mémorisés réappliqués` dans le succès.

Pas d'autre changement. Rien de touché côté contract_engine, YAML,
transforms, unknown_columns (on utilise `unkcol.load_learned_patterns`
qui existait déjà depuis 4.1.7).

## À tester après déploiement

1. Supprimer tous les fichiers, uploader un Ayvens EP, mapper les
   champs demandés, cliquer 💾 Mémoriser ce format.
2. Cliquer « 🔄 Nouvel import » pour reset la session.
3. Re-uploader **le même Ayvens EP**. Le bandeau
   `🗂️ Mappings mémorisés réappliqués` doit apparaître, et les cartes
   « Colonne non identifiée » ne doivent plus demander ces champs.
4. (Sanity) Uploader un fichier dont le mapping n'a **jamais** été
   mémorisé. Aucun bandeau 🗂️. Comportement inchangé.

## Suite

4.3.0 résout le bug de fond. 4.3.1 ajoutera l'écran d'audit des
mappings (un seul écran, 2 chemins d'accès — bouton dans les onglets
Véhicules/Contrats + nouveau chapitre « Mappings mémorisés » dans le
menu gauche).
