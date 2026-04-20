# Revio Onboarding Tool

Outil interne pour automatiser la préparation des fichiers d'import Revio à partir
des fichiers hétéroclites reçus des clients (fichiers internes, exports loueurs,
API Plaques).

## Ce que fait l'outil

1. Tu drag-and-drop tous les fichiers reçus pour un client (CSV ou Excel).
2. L'outil détecte automatiquement le type de chaque fichier (fichier client,
   Etat de parc Ayvens, UAT Arval, API Plaques, etc.).
3. Un LLM (Claude) propose un mapping des colonnes vers les templates Revio.
   Tu valides ou corriges en un clic.
4. Tu découpes les agences détectées en flottes (une flotte = un dossier Revio).
5. L'outil normalise tout ce qui traîne (dates dans tous les formats, plaques
   avec/sans tirets, montants avec €, km en milliers ou unités, etc.).
6. Il produit les 4 CSV Revio par flotte (`vehicle.csv`, `driver.csv`,
   `contract.csv`, `asset.csv`) plus un rapport d'erreurs, dans un .zip.

## Installation (sur ton Mac)

Prérequis : avoir Python 3.10+ installé (tape `python3 --version` dans un Terminal,
si ça te sort un numéro > 3.10, tu es bon).

```bash
# 1. Ouvre un Terminal dans le dossier du projet.
cd /chemin/vers/revio_onboarding

# 2. Crée un environnement virtuel (isole les dépendances).
python3 -m venv .venv
source .venv/bin/activate

# 3. Installe les dépendances.
pip install -r requirements.txt

# 4. Copie le fichier d'exemple de configuration.
cp .env.example .env

# 5. Ouvre .env dans un éditeur et colle ta clé Anthropic :
#    ANTHROPIC_API_KEY=sk-ant-... (obtenue sur https://console.anthropic.com/)
```

## Lancement

```bash
# Depuis le dossier du projet, environnement virtuel activé :
streamlit run app.py
```

Ton navigateur s'ouvre sur `http://localhost:8501`. L'app tourne en local tant que
le Terminal est ouvert.

## Flux d'utilisation

1. **Sidebar** : saisis le nom du client (ex. `YSEIS`). Vérifie que la clé Anthropic
   est bien détectée.
2. **Étape 1** : dépose tous les fichiers (client + loueurs + API Plaques).
3. **Étape 2** : vérifie que chaque fichier a bien été identifié et pointe vers le
   bon schéma Revio (`vehicle`, `driver`, `contract`, `asset`). Ignore les
   fichiers fiscaux (TVS, TVU) qui ne servent pas à l'import.
4. **Étape 3** : pour chaque fichier, clique sur *Proposer un mapping (IA)*. Révise
   les associations proposées.
5. **Étape 4** : l'outil liste les agences trouvées. Groupe-les en flottes.
6. **Étape 5** : télécharge le .zip avec tous les CSV prêts à importer.

## Instructions spéciales

Dans la sidebar, tu peux écrire des règles en langage naturel qui s'appliquent à
tout l'onboarding. Exemples :

- *"Pour ce client, les VP-BR sont à classer en `service`, pas en `private`."*
- *"Si la Date fin de contrat est vide, calcule-la depuis Date début + Durée (en mois)."*
- *"La colonne `Moteur` de ce client utilise G/D/E (Gazole/Diesel/Essence) au lieu de T/H/E."*

Ces instructions sont injectées dans le prompt du LLM au moment du mapping.

## Structure du code

```
revio_onboarding/
├── app.py               # UI Streamlit (le seul fichier "interface")
├── src/
│   ├── schemas.py       # Les 4 templates Revio cibles
│   ├── partners.py      # UUIDs des loueurs (Ayvens, Arval, etc.)
│   ├── normalizers.py   # Nettoyage dates/plaques/montants/km
│   ├── detectors.py     # Détection automatique du type de fichier
│   ├── llm_mapper.py    # Mapping de colonnes via Claude
│   ├── pipeline.py      # Orchestration : charge, mappe, fusionne, valide
│   └── output_writer.py # Génération du .zip final
├── requirements.txt
├── .env.example
└── .streamlit/config.toml
```

## À faire

- [ ] Intégration Google Drive (création du dossier client + upload des CSV).
- [ ] Déploiement sur Streamlit Cloud avec auth équipe.
- [ ] Mémorisation des mappings par client (ex. YSEIS se souvient du mapping du client).
- [ ] Ajout des UUIDs manquants dans `src/partners.py`.
- [ ] Mapping `usage` VS/VP/VP-BR → utility/service/private (à trancher avec Augustin).
- [ ] Support de nouveaux loueurs au fur et à mesure qu'ils apparaissent.

## Clé Anthropic

Tu peux obtenir une clé sur https://console.anthropic.com/. Coût estimé : quelques
centimes par onboarding (les prompts sont petits, les réponses aussi).

Tu peux plafonner la consommation dans les réglages Anthropic pour dormir tranquille.
