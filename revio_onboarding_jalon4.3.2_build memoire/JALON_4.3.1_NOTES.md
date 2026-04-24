# Jalon 4.3.1 — Écran d'audit des mappings mémorisés

**Date** : 2026-04-24
**Scope** : `app.py`, `src/unknown_columns.py`
**Contexte** : suite du Jalon 4.3.0. 4.3.0 a fixé la plomberie (re-lecture
de `learned_columns.yml` à l'upload). 4.3.1 ajoute l'UX de vérification /
correction des mappings, selon la spec Augustin du 24 avril.

---

## Le besoin

Augustin :

> *"Sur les fichiers pour lesquels on a enregistré en dur le mapping des
> champs je ne vois pas de corbeille. Avant de passer au LLM je voudrais
> résoudre ce problème. [...] Auto-matching doit continuer comme
> aujourd'hui. Mappings saved doivent rester. Doit pouvoir vérifier,
> modifier une erreur, sauvegarder, sans écraser les autres champs."*

Design validé ensemble (après discussion) :

- **Un seul écran** atteignable par 2 chemins.
- **Tableau 1 ligne = 1 colonne du fichier source**, avec cibles
  multi-tables (Véhicules + Contrats) et état (auto / session / mémorisé).
- **Suppression = retour auto-détection** (l'algo retente au prochain
  upload, si la colonne est toujours là elle revient toute seule).
- **Véhicules + Contrats uniquement** pour l'instant (Driver / Assurance
  viendront quand les moteurs seront là).

## Chemin n°1 — bouton contextuel dans Véhicules / Contrats

Dans chaque onglet (Vehicles et Contracts), sous la zone d'import et les
cartes de mapping manuel existantes, nouveau bouton :

```
🔎 Vérifier les correspondances
```

Cliqué, il déplie un bloc qui contient :

- 1 expander par fichier uploadé (étiqueté `📄 <filename> — <slug>`)
- Dans chaque expander, le tableau d'audit :

```
| Colonne fichier       | Mappé vers                       | État        | Action |
|-----------------------|----------------------------------|-------------|--------|
| N° Contrat            | [ contract.number ]              | 🧠 mémorisé | 🗑️    |
| Plaque                | [ vehicle.registrationPlate,     | 🧠 mémorisé | 🗑️    |
|                       |   contract.plate ]               |             |        |
| Marq                  | [ vehicle.brand ]                | ✎ session  | 🗑️    |
| Date début            | [ contract.startDate ]           | 🧠 mémorisé | 🗑️    |
| (colonne sans binding)| —                                | —           | —      |
```

**UX des contrôles** :

- **Multiselect** dans la colonne "Mappé vers" : liste déroulante de
  toutes les cibles possibles (vehicle.* + contract.*). L'utilisateur
  peut cocher/décocher → le mapping session s'actualise en live.
- **État** : badge coloré
  - 🧠 mémorisé (vert) — présent dans `learned_columns.yml`
  - ✎ session (bleu) — changement non persisté
  - — (gris) — pas de binding
- **🗑️** : efface tous les bindings pour cette ligne, en session ET en
  mémoire (retour auto-détection au prochain upload).

En bas du tableau, 2 boutons globaux :

- **💾 Mémoriser toutes les modifications** — écrit tous les bindings
  actuels de ce fichier dans `learned_columns.yml` (+ commit GitHub).
- **🗑️ Effacer toute la mémoire de ce slug** — wipe tout pour ce type
  de fichier (gardé séparé du 🗑️ par ligne pour clarté).

## Chemin n°2 — chapitre "Mappings mémorisés" dans le menu gauche

Nouveau chapitre ajouté à `NAV_ITEMS` :

```
🗂️ Mappings mémorisés
```

Page globale :

```python
render_mappings_audit_page()
```

Liste tous les slugs qui ont au moins un mapping mémorisé. Chaque slug
ouvre un expander avec la même structure de tableau que le chemin 1,
**mais en lecture + 🗑️ seulement** (pas d'édition — on n'a pas de df
ouvert pour proposer des colonnes source alternatives).

Explicite dans la caption : *"Pour modifier un mapping, va dans
Import — Moteur de règles, uploade le fichier, puis clique Vérifier les
correspondances."*

## Back-end — 2 nouvelles fonctions dans `unknown_columns.py`

```python
def unregister_learned_column(
    patterns_path, *, table, source_slug, field_name,
) -> bool:
    """Supprime un unique (table, slug, field) entry.
    Idempotent ; True si supprimé, False si déjà absent.
    Prune les nœuds vides (slug et table) pour ne pas accumuler de
    clés fantômes."""
```

```python
def unregister_learned_slug(
    patterns_path, *, source_slug, tables=("vehicle", "contract"),
) -> int:
    """Wipe all memorized mappings for a slug, toutes tables confondues.
    Renvoie le nombre d'entries supprimées (feedback UX)."""
```

### Tests

Test suite reproduite depuis un vrai `learned_columns.yml` à 4
entrées (2 contract + 1 vehicle + 1 arval) :

```
>>> unregister_learned_column(p, table='contract',
...                           source_slug='ayvens_etat_parc',
...                           field_name='number')
True                  # supprimé
>>> unregister_learned_column(...)  # même appel
False                 # idempotent
>>> unregister_learned_column(p, table='contract',
...                           source_slug='ayvens_etat_parc',
...                           field_name='startDate')
True
# contract.ayvens_etat_parc est maintenant vide → pruné
>>> unregister_learned_slug(p, source_slug='ayvens_etat_parc')
1                     # ne restait que vehicle.brand
# Aucun ayvens_etat_parc nulle part
```

ALL TESTS PASS.

## Précédence et invalidation

- Modifier un mapping dans le tableau contextuel invalide
  `engine_result` + `contract_result` → l'utilisateur doit re-cliquer
  Appliquer pour voir l'effet. C'est volontaire : on évite de laisser
  un résultat stale alors que les mappings ont changé.
- Un 🗑️ **ne réinvalide pas** automatiquement les patterns Vehicle
  (`learned_patterns.yml`). Si Vehicle a un `column_mapping` qui écrase
  la suppression à l'upload suivant, il faut supprimer le pattern lui-
  même via 🗑️ Supprimer le pattern (Jalon 4.2.7, expander vue
  "reconnu"). Une version future unifiera ces 2 suppressions.

## Ce que ça change pour Augustin

- Dans le tableau, tu vois **en une seule vue** tous les mappings
  actuellement actifs sur un fichier, leur état, et tu peux les
  modifier un par un sans rien toucher d'autre.
- Plus besoin d'aller dans la carte d'inconnu puis cliquer Mémoriser :
  pour une correction, tu décoches l'ancienne cible dans la colonne
  d'origine et tu coches la nouvelle cible dans la bonne colonne.
- Depuis le menu gauche, tu as **la liste globale** des mappings
  mémorisés — tu peux voir d'un coup d'œil ce qui est persisté côté
  app, et supprimer individuellement une entrée que tu juges erronée.

## Fichiers modifiés

- `src/unknown_columns.py`
  - `+ unregister_learned_column` (~70 lignes)
  - `+ unregister_learned_slug` (~25 lignes)
- `app.py`
  - `+ _LEARNED_COLUMNS_PATH` (module-level constant)
  - `+ _all_audit_target_fields` (source de vérité pour le multiselect)
  - `+ _load_memorized_for_slug` (lecture yaml par slug)
  - `+ _build_audit_rows` (inversion engine_overrides → lignes par src_col)
  - `+ _state_badge` (badge HTML mémorisé / session / —)
  - `+ _render_audit_table_contextual` (le tableau éditable)
  - `+ _render_audit_table_sidebar` (lecture seule + 🗑️)
  - `+ _list_memorized_slugs` (liste des slugs avec mappings)
  - `+ render_mappings_audit_page` (page du menu gauche)
  - `+ _render_audit_button_section` (bouton dans les onglets)
  - 2 call-sites : Vehicle tab + Contract tab
  - `NAV_ITEMS` : ajout du chapitre `mappings`
  - Routing : ajout du `elif mode == "mappings"`

Pas de YAML touché, pas de moteur touché.

## À tester après déploiement

1. Upload un Ayvens EP, mémoriser quelques mappings (flow existant).
2. Aller dans l'onglet Véhicules, cliquer **🔎 Vérifier les
   correspondances**. Le tableau doit apparaître avec toutes les
   colonnes du fichier, les bindings en badges, état "🧠 mémorisé" sur
   les champs déjà mémorisés.
3. Décocher une cible dans le multiselect d'une ligne → l'état doit
   passer à "✎ session" et le moteur doit se désinvalider.
4. Cliquer 🗑️ sur une ligne → tous les bindings pour cette colonne
   disparaissent.
5. Cliquer 💾 Mémoriser toutes les modifications → success banner
   "✓ N champs mémorisés".
6. Aller dans le menu gauche **🗂️ Mappings mémorisés**. Le slug doit
   apparaître avec toutes ses entrées. 🗑️ par ligne supprime
   individuellement.
7. Cliquer 🗑️ Effacer toute la mémoire de ce slug → success banner +
   le slug disparaît de la page globale.

## Limitations connues (pour 4.3.2+)

- Les `column_mapping` stockés dans les patterns Vehicle
  (`learned_patterns.yml`) ne sont pas éditables depuis ce tableau :
  pour un pattern file-signature, il faut passer par 🗑️ Supprimer le
  pattern ou refaire le mapping complet via l'onglet Véhicules et
  ré-Mémoriser. Unification prévue plus tard.
- Pas de confirmation sur les suppressions. Si tu cliques 🗑️ par
  erreur, refais le mapping et re-Mémoriser. (La source est toujours là,
  c'est juste un choix à refaire.)
- Sidebar view : édition limitée à la suppression. Pour modifier,
  upload → onglet → contextuel.
