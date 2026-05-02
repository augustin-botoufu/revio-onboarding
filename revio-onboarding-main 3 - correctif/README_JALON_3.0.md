# Jalon 3.0 — Segmentation par flottes / agences

## Ce qui change

Nouvelle fonctionnalité : segmenter un import en N flottes (agences,
centres de coût, établissements…) à partir d'une colonne du fichier
client.

### UX côté moteur (page "Moteur")

1. **Étape 4** — nouveau bouton `🏢 Segmenter en flottes` à côté du
   bouton "Appliquer les règles Vehicle". Un badge indique combien
   de flottes sont actives.
2. **Popup** (type `st.dialog`) :
   - Sélection du fichier source (pré-rempli avec `client_file`)
   - Sélection de la colonne (suggestions : Agence, Code CC, Centre
     de coût, Établissement, Site, Direction…)
   - Pour chaque valeur unique : choix du nom de flotte cible
     (laisser vide = ignoré)
   - Deux valeurs pointées vers la même flotte = fusionnées
   - Les cellules vides restent visibles et ne font perdre aucun
     véhicule
3. **Étape 6** — un **unique bouton** `📦 Télécharger le pack
   d'import (.zip)` remplace les deux anciens téléchargements.

### Structure du zip

```
revio_import_<client>_<YYYYMMDD_HHMM>/
├── onboarding_complet.xlsx   (multi-onglets : Sommaire + tous + 1 par flotte)
├── rapport.xlsx               (optionnel — rapport par cellule)
└── vehicles/
    ├── vehicles_tous.csv
    ├── vehicles_<slug>.csv    (un par flotte, uniquement si segmentation active)
    └── vehicles_non-rattache.csv  (plaques sans flotte, si il y en a)
```

Pas de segmentation → pas de sous-onglets ni de CSV par flotte.
Aucun artefact vide.

## Fichiers inclus

### Nouveaux
- `src/fleet_segmentation.py` — logique pure (unique values, mapping,
  split, slugify, sheet-name sanitization)
- `src/zip_writer.py` — builder zip + master xlsx
- `tests/test_fleet_segmentation.py` — 18 tests
- `tests/test_zip_writer.py` — 10 tests

### Modifié
- `app.py` — import des 2 nouveaux modules, session state étendue,
  `@st.dialog` `_fleet_segmentation_dialog()`, bouton + badge étape 4,
  étape 6 refactorée en un seul bouton zip

## Installation

Dans le repo GitHub :

```bash
# À la racine du repo
cp /chemin/vers/jalon30_patch/app.py .
cp /chemin/vers/jalon30_patch/src/*.py src/
cp /chemin/vers/jalon30_patch/tests/*.py tests/

git add app.py src/fleet_segmentation.py src/zip_writer.py \
        tests/test_fleet_segmentation.py tests/test_zip_writer.py
git commit -m "Jalon 3.0 — segmentation par flottes + zip unique"
git push
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Doit renvoyer `Ran 98 tests ... OK`.
