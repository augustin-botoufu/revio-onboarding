# Auth + Reset Session — patch

Deux features dans le même patch :

## 1. 🔒 Accès restreint par mot de passe (tâche #9)

Un écran de login bloque l'accès à l'app. Le mot de passe est stocké
dans les Secrets Streamlit Cloud (jamais sur GitHub) et peut être
changé à chaud. Une fois connecté, un **cookie signé** garde la
session active pendant **7 jours** même après refresh / fermeture
d'onglet. Rotation du mot de passe = invalidation immédiate de
toutes les sessions.

### Déploiement — à faire côté Streamlit Cloud

1. Dans le dashboard Streamlit Cloud → ton app → Settings → Secrets
2. Ajoute une ligne :
   ```toml
   app_password = "un-mot-de-passe-que-tu-choisis"
   ```
3. Save → redeploy
4. Ouvre l'app — tu dois tomber sur l'écran "Accès restreint"

### Comportement
- **Pas de `app_password` configuré** → mode dev, accès libre (+ bannière d'info dans la sidebar)
- **`app_password` configuré** → login requis, cookie 7 jours

## 2. 🔄 Bouton "Nouvel import" (tâche #31)

Nouveau bouton dans la sidebar : **vide tous les fichiers / mappings /
règles en cours** sans rafraîchir la page. Garde :
- L'authentification (on reste connecté)
- La navigation (onglet actif)
- Les paramètres globaux (diagnostics GitHub)

Plus besoin de faire F5 entre deux onboardings → la session reste
propre, et le cookie d'auth est conservé.

## Fichiers inclus

### Nouveaux
- `src/auth.py` — login gate + cookie manager
- `src/session_reset.py` — logique du reset (whitelist-based, pure)
- `tests/test_session_reset.py` — 9 tests

### Modifiés
- `app.py` — import auth + reset, `require_password()` après
  `set_page_config`, boutons "Nouvel import" + "Déconnexion" dans la
  sidebar
- `requirements.txt` — ajout de `extra-streamlit-components>=0.1.70`

## Installation

```bash
# À la racine du repo
cp /chemin/vers/jalon_auth_reset_patch/app.py .
cp /chemin/vers/jalon_auth_reset_patch/requirements.txt .
cp /chemin/vers/jalon_auth_reset_patch/src/auth.py src/
cp /chemin/vers/jalon_auth_reset_patch/src/session_reset.py src/
cp /chemin/vers/jalon_auth_reset_patch/tests/test_session_reset.py tests/

git add app.py requirements.txt \
        src/auth.py src/session_reset.py \
        tests/test_session_reset.py
git commit -m "Auth par mot de passe + bouton Nouvel import"
git push
```

Streamlit Cloud va auto-redeployer. Après le redeploy :
1. Settings → Secrets → ajouter `app_password = "..."`
2. Save → redeploy (encore)
3. Partage le mot de passe à l'équipe (1Password recommandé)

## Tests

```bash
python3 -m unittest discover -s tests -v
```

Attendu : `Ran 107 tests ... OK` (107 = 98 anciens + 9 nouveaux).
