# Guide de déploiement pas-à-pas (pour Augustin)

Objectif : avoir l'outil accessible sur une URL type `revio-onboarding.streamlit.app`
que toi et ton équipe utilisez dans un navigateur. Aucune installation sur ton Mac.

Temps total estimé : 45 min à 1h, la première fois.

---

## Phase 1 — Clé API Anthropic (5 min)

C'est ce qui permet à l'outil d'utiliser Claude pour proposer le mapping des colonnes.

1. Va sur <https://console.anthropic.com/> (compte différent de Claude.ai).
2. Crée un compte (email + mot de passe). Confirme l'email.
3. Va dans **Settings → Billing** et ajoute un moyen de paiement. Mets 5 à 10 € de crédit (ça fait largement des centaines d'onboardings).
4. Va dans **API Keys** → **Create Key**. Donne-lui le nom `revio-onboarding-prod`. Copie la clé (elle commence par `sk-ant-...`).
5. **Garde cette clé quelque part en sécurité (ex. ton gestionnaire de mots de passe)**. Anthropic ne te la ré-affichera jamais.

---

## Phase 2 — Mettre le code sur GitHub (15 min)

GitHub est un "Google Drive pour le code". Tout se fait dans le navigateur.

1. Va sur <https://github.com/> et crée un compte (gratuit). Utilise ton email Revio.
2. Une fois connecté, en haut à droite clique sur le `+` → **New repository**.
3. Remplis :
   - **Repository name** : `revio-onboarding`
   - **Visibility** : **Private** (important - le code reste privé).
   - Coche **Add a README file**.
   - Clique **Create repository**.
4. Tu arrives sur la page du repo. Clique sur **Add file** (en haut) → **Upload files**.
5. Ouvre le dossier `revio_onboarding` sur ton Mac (celui que je t'ai préparé).
6. Sélectionne **tout le contenu du dossier** (Cmd+A). Glisse-dépose dans la fenêtre GitHub.
   - ⚠️ **NE dépose PAS** le fichier `.env` s'il existe chez toi (il contient la clé API qu'on va mettre ailleurs, en sécurité).
   - Le `.gitignore` est déjà configuré pour l'exclure, mais vérifie quand même.
7. En bas de la page, clique **Commit changes**.

Ton code est maintenant sur GitHub. ✓

---

## Phase 3 — Déployer sur Streamlit Cloud (15 min)

Streamlit Cloud prend ton code GitHub et le fait tourner sur leurs serveurs, gratuitement.

1. Va sur <https://share.streamlit.io/>.
2. Clique **Continue with GitHub**. Autorise Streamlit à accéder à tes repos.
3. Sur le dashboard, clique **Create app** → **Deploy a public app from GitHub**.
   - Même si le repo est privé, l'accès à l'app peut être public ou restreint.
4. Remplis :
   - **Repository** : `ton-pseudo/revio-onboarding`
   - **Branch** : `main`
   - **Main file path** : `app.py`
   - **App URL** (optionnel) : tu peux personnaliser, ex. `revio-onboarding`.
5. Avant de cliquer Deploy, clique sur **Advanced settings**.
6. Dans **Secrets**, colle :
   ```toml
   ANTHROPIC_API_KEY = "sk-ant-TA-CLE-ICI"
   ```
   (remplace par la vraie clé de la Phase 1).
7. Clique **Save**, puis **Deploy**.
8. Attends 2-3 min. Streamlit installe les dépendances, puis ton app se lance.

Tu as maintenant une URL publique type `https://revio-onboarding.streamlit.app`.

---

## Phase 4 — Restreindre l'accès à ton équipe (facultatif, 5 min)

Par défaut l'URL est publique. Pour la restreindre :

1. Sur le dashboard Streamlit, clique sur ton app → **Settings**.
2. Dans **Sharing**, active **Only specific people can view this app**.
3. Ajoute les emails de ton équipe (l'adresse avec laquelle ils se connectent à Streamlit via GitHub ou email).

---

## Phase 5 — Mettre à jour l'outil plus tard

Quand je corrige un bug ou ajoute une feature :

1. Je te donne les fichiers mis à jour.
2. Sur GitHub, tu navigues au fichier concerné → icône crayon → colle le nouveau contenu → **Commit changes**.
3. Streamlit détecte automatiquement le changement et redéploie en 1 min.

---

## En cas de pépin

- **"ImportError" ou "ModuleNotFoundError" dans les logs Streamlit** : le `requirements.txt` n'a pas été uploadé. Vérifie qu'il est bien dans le repo GitHub.
- **L'app se lance mais "Clé Anthropic non détectée"** : retourne dans **Settings → Secrets** sur Streamlit et vérifie le format exact (`ANTHROPIC_API_KEY = "sk-ant-..."` avec les guillemets).
- **Rien ne marche** : envoie-moi une capture d'écran du message d'erreur, je débugge.
