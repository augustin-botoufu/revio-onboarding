# Jalon 3 — Governance multi-user (esquisse)

> Document de travail préparatoire. Pas de code encore — à relire avant de lancer
> le chantier pour valider le choix de backend et le workflow.

## 1. Pourquoi ce jalon existe

Jalon 2.5 (auto-commit GitHub) te couvre en solo. Dès que Sales + AM utilisent
l'outil, 3 problèmes apparaissent :

1. **Écritures concurrentes sur le repo** — deux personnes qui mémorisent en
   même temps, c'est gérable en solo (retry auto) mais ça devient bruyant à 5+.
2. **Pas de revue** — un Sales qui mémorise "n'importe quoi" part en prod sans
   filet. Tu veux un état "brouillon" avant approbation.
3. **Redéploiement Streamlit à chaque pattern** — OK pour 3 patterns/semaine,
   insupportable à 30/semaine (1 min de lag × 30 = tout le monde attend).

Le passage en base de données adresse les trois d'un coup : écritures
atomiques, statut `draft/pending/approved`, lecture en runtime sans redeploy.

## 2. Choix de backend

Décision binaire : **Supabase** vs **Turso**. Les deux sont gratuits jusqu'à un
volume qu'on n'atteindra jamais. Recommandation : **Supabase**.

| Critère | Supabase (Postgres) | Turso (SQLite) |
|---|---|---|
| Setup | 5 min (dashboard web) | 10 min (CLI à installer) |
| SDK Python | `supabase-py` stable | `libsql-client` moins mature |
| Auth intégrée | Oui (GitHub, Google, magic link) | Non, à coder |
| Row-Level Security | Oui (policies SQL) | Non |
| Dashboard pour voir/éditer les données | Excellent | Basique |
| Coût à l'échelle | Free tier généreux (500 MB, 2 GB transfert) | Free tier plus large mais moins utile ici |
| Portabilité hors Supabase | Postgres standard | SQLite standard (plus simple à exporter) |

**Recommandation : Supabase**, parce que :
- L'auth intégrée te fait gagner le Jalon #9 ("Restreindre l'accès à l'app")
  au passage — un seul auth au lieu de deux.
- Le dashboard web sert d'admin panel gratuit : tu peux lire/éditer les
  patterns à la main depuis ton browser sans builder une UI.
- Row-Level Security te permet de distinguer ce que Sales voient vs ce
  qu'AM voient, sans code applicatif.

## 3. Schéma de données (Postgres)

```sql
-- Table principale : un pattern = une ligne
create table learned_patterns (
    id              text primary key,              -- l'id actuel (slug du filename)
    slug            text not null,                 -- 'autre_loueur_etat_parc', etc.
    loueur_hint     text,                          -- 'Alphabet', 'Leasys', ...
    filename_regex  text not null,
    columns_include jsonb default '[]',
    header_row      int,
    column_mapping  jsonb default '{}',

    status          text not null default 'draft'
                    check (status in ('draft', 'pending', 'approved', 'archived')),
    created_at      timestamptz not null default now(),
    created_by      text,                          -- email
    updated_at      timestamptz not null default now(),
    approved_at     timestamptz,
    approved_by     text,

    -- Métriques d'usage (pour repérer les patterns morts plus tard)
    match_count     int not null default 0,
    last_matched_at timestamptz
);

create index on learned_patterns(slug);
create index on learned_patterns(status);

-- Audit trail : chaque modif est loggée (utile pour "qui a cassé quoi ?")
create table pattern_audit (
    id           bigserial primary key,
    pattern_id   text not null,
    action       text not null check (action in ('create', 'update', 'approve', 'archive', 'delete')),
    actor        text not null,                    -- email
    at           timestamptz not null default now(),
    before       jsonb,                            -- snapshot avant
    after        jsonb                             -- snapshot après
);

create index on pattern_audit(pattern_id);
```

**Pourquoi `status`** :
- `draft` : Sales vient de mémoriser, pas encore validé. Appliqué uniquement
  pour le user qui l'a créé (ne pollue pas les autres).
- `pending` : Sales demande approbation à Augustin/AM.
- `approved` : visible et appliqué pour tout le monde.
- `archived` : retiré sans le supprimer (historique conservé).

## 4. Workflow UI proposé

### Côté Sales (usage normal)
1. Upload fichier → reconnaissance auto (si pattern `approved` matche).
2. Mapping (IA ou manuel) → bouton "💾 Mémoriser ce format".
3. Ça sauve un **draft** associé à son email. Le draft est actif pour **lui
   uniquement** les prochaines fois.
4. Bouton "📤 Soumettre pour approbation" → passe en `pending`.

### Côté Augustin/AM (revue)
- Nouvelle page `🔍 Patterns à approuver` dans la sidebar.
- Liste des `pending` avec aperçu + qui l'a soumis + quand.
- Sanity checks auto affichés :
  - "Ce regex matche déjà 3 autres patterns — doublon potentiel"
  - "Seulement 1 champ mappé — est-ce intentionnel ?"
  - "Utilise les colonnes [X, Y] qu'aucun autre pattern n'utilise — OK"
- Boutons : ✅ Approuver / ✏️ Demander modif / 🗑️ Rejeter.

### Canary (optionnel, Jalon 3.5)
Un pattern `approved` ne s'applique d'abord qu'aux 3 prochains fichiers, pour
collecter des métriques (taux de match, nb colonnes trouvées). Si vert → vrai
`approved`. Sinon → auto-revert en `pending` avec une note.

## 5. Migration YAML → DB

Script one-shot :

```python
# migrate_yaml_to_db.py
from src.learned_patterns import load_patterns
from src.db import patterns_repo  # nouveau module

for p in load_patterns("src/rules/learned_patterns.yml"):
    patterns_repo.insert(
        id=p.id, slug=p.slug, loueur_hint=p.loueur_hint,
        filename_regex=p.filename_regex,
        columns_include=p.columns_include,
        column_mapping=p.column_mapping,
        status="approved",                    # on considère que tout ce qui est dans le YAML est déjà validé
        created_by="augustin@gorevio.co",
        approved_at=now(), approved_by="augustin@gorevio.co",
    )
```

Le YAML reste en place pendant 2 semaines après la migration comme fallback
read-only, au cas où la DB est down. Après, on le supprime.

## 6. Estimations

| Étape | Durée |
|---|---|
| Setup Supabase (projet, table, secrets) | 30 min |
| Module `src/db/patterns_repo.py` (CRUD + auth) | 1h |
| Remplacement `learned_patterns.load_patterns` par `patterns_repo.list_approved` | 30 min |
| UI : page "Patterns à approuver" | 1h |
| UI : bouton "Soumettre pour approbation" (remplace le commit GitHub) | 30 min |
| Migration YAML → DB + script de rollback | 30 min |
| Tests E2E | 1h |
| **Total** | **~5h** |

Soit une grosse demi-journée focus.

## 7. Décisions en attente

Avant de lancer, valider :

1. **Supabase ou Turso ?** (reco : Supabase)
2. **Qui approuve ?** Toi seul, ou AM aussi ? (reco : toi seul au démarrage,
   AM plus tard quand confiance)
3. **Drafts visibles où ?** Uniquement au user qui a créé (isolé) ou aussi
   aux autres en mode "bêta" ? (reco : isolé, sinon c'est le bordel)
4. **Garde-t-on Jalon 2.5 en parallèle** comme fallback si la DB est down ?
   (reco : non, c'est de la dette technique à supprimer)

## 8. Ce qui rend Jalon 2.5 throwaway

Concrètement, au passage en Jalon 3, tu supprimes :
- `src/github_sync.py` (complet)
- `tests/test_github_sync.py`
- Le bouton "📋 Mémoriser ce format" branché sur GitHub → rebranché sur la DB
- Les secrets `GITHUB_TOKEN`, `GITHUB_REPO` dans Streamlit

Tu gardes :
- `src/learned_patterns.py` : la classe `LearnedPattern` et `build_pattern_entry`
  restent utiles (le repo DB renvoie des `LearnedPattern`)
- `match_pattern()` : logique pure, inchangée
- Le format `column_mapping` : identique à ce qu'on stocke en DB

Donc sur les ~500 lignes de code Jalon 2.5, environ 300 sont jetées. C'est
acceptable vu que ça te permet d'être opérationnel en solo cette semaine.
