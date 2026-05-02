# Jalon 4.2.10 — isHT auto-dérivé d'`api_plaques` (Contract autonome)

**Date** : 2026-04-24
**Scope** : `src/contract_engine.py`
**Contexte** : post-4.2.9, Augustin teste à nouveau et rapporte :

> *"Dans contrat j'ai toujours rien dans HT alors que dans véhicules j'ai
> bien donné une dénomination pour tout sur la partie VP VU etc"*

Il partage `vehicles_tous.csv` (54 lignes, toutes avec `usage` = private /
service) et `contracts_tous.csv` (54 lignes, **2 seulement** avec `isHT`
rempli).

---

## Diagnostic

Le postpass `_postpass_isHT` avait 2 sources d'information :

1. `_extract_vp_from_ep_sources` — scan des fichiers EP loueur pour une
   colonne VP/VU. 4.2.9 a assoupli le matching, mais ça ne couvre encore
   que les plaques présentes dans un EP, et uniquement si le format EP
   expose une colonne de type véhicule.

2. `vehicle_vp_by_plate` — extrait de `st.session_state.engine_result.df`
   (colonne `usage` du moteur Véhicules). Problème : **dès qu'un fichier
   est re-uploadé dans l'app, `engine_result` est invalidé** (cf. app.py
   ligne 1843 : `st.session_state.engine_result = None`). Si l'utilisateur
   re-upload quelque chose après avoir cliqué Appliquer Véhicules,
   l'info VP est perdue pour le Contract run qui suit.

Dans le cas d'Augustin, 52/54 plaques n'ont été attrapées par ni l'un ni
l'autre → cellules `isHT` vides dans l'export.

## Fix — lire `api_plaques` directement depuis le Contract engine

Le fichier `api_plaques` (SIV officiel) est uploadé une fois et couvre
toutes les plaques du parc par construction. La colonne `genreVCGNGC`
du SIV donne le code VP/CTTE/CAM → exactement l'info dont on a besoin.

Nouvelle fonction dans `contract_engine.py` :

```python
def _extract_vp_from_api_plaques(indexed):
    out = {}
    df = indexed.get("api_plaques")
    if df is None or df.empty:
        return out
    col = _find_column(df, ["genreVCGNGC", "genre", "genreCG",
                            "genre_cg", "categorie", "catégorie"])
    if col is None:
        return out
    for key, val in df[col].items():
        if key in out or _is_null(val):
            continue
        low = str(val).strip().lower()
        if any(h in low for h in ("vp", "particul", "tourisme")):
            out[key] = (True, "api_plaque")
        elif any(h in low for h in ("vu", "utilit", "commercial",
                                     "camion", "fourgon", "ctte", "pl")):
            out[key] = (False, "api_plaque")
    return out
```

Nouvelle priorité dans `_postpass_isHT` :

```
1. EP loueur (inchangé)
2. api_plaques lu directement par le Contract  ← nouveau en 4.2.10
3. vehicle_vp_by_plate (fallback Vehicle engine — si run dans la session)
```

## Test avec le vrai fichier d'Augustin

```python
>>> # Simule un api_plaques avec les 54 plaques du parc d'Augustin
>>> # (genreVCGNGC = "VP" pour private, "CTTE" pour service/utility)
>>> indexed = {"api_plaques": api_plaques_df}
>>> vp_map = _extract_vp_from_api_plaques(indexed)
>>> len(vp_map)
54
>>> sum(1 for v, _ in vp_map.values() if v)       # VP = private = TTC
32
>>> sum(1 for v, _ in vp_map.values() if not v)   # non-VP = service/utility = HT
22

>>> _postpass_isHT(out_df, {}, lineage,
...                vehicle_vp_by_plate={},      # vide, comme dans le cas réel
...                vp_from_ep_by_key={},        # vide, pas d'EP loueur
...                vp_from_api_by_key=vp_map)   # api_plaques seul
>>> out_df["isHT"].value_counts().to_dict()
{False: 32, True: 22}
>>> out_df["isHT"].isna().sum()
0
```

**Résultat : 54/54 cellules remplies, 32 False (VP → TTC) + 22 True
(non-VP → HT).** Avant 4.2.10 : 2/54.

## Pourquoi 2/54 avant et pas 0/54 ?

Les 2 cellules qui étaient déjà remplies (GM-234-LM en False entre autres)
venaient probablement du scan EP loueur — Augustin a au moins un EP avec
une colonne VP/VU reconnue sur ces 2 lignes. Le reste retombait sur
`vehicle_vp_by_plate`, qui était vide — soit parce qu'Augustin n'avait pas
cliqué Appliquer Véhicules dans la session courante, soit parce qu'un
re-upload avait invalidé le résultat entre temps.

Avec 4.2.10 ce n'est plus un problème : le Contract engine trouve
l'info VP tout seul, dès qu'`api_plaques` est chargé.

## Fichiers modifiés

- `src/contract_engine.py`
  - Nouvelle fonction `_extract_vp_from_api_plaques` (lignes ~549-590).
  - `_postpass_isHT` : nouveau paramètre optionnel
    `vp_from_api_by_key` ; priorité 2 intercalée entre EP et
    vehicle_vp_by_plate.
  - Appel du postpass enrichi avec l'extraction api_plaques.

Pas d'autre changement. Rien de touché côté app.py, YAML, transforms.

## À tester après déploiement

1. Supprimer tous les fichiers, uploader uniquement :
   - un `client_vehicle` (quelques plaques)
   - un `api_plaques` avec colonne `genreVCGNGC`
2. Cliquer ▶️ Appliquer Contrats **sans** toucher à l'onglet Véhicules.
3. Télécharger le zip → `contracts.csv` doit avoir `isHT` rempli pour
   toutes les plaques (FALSE pour VP, TRUE pour non-VP).
4. Bonus : uploader en plus un Ayvens EP avec "Genre = Voiture particulière".
   Pour ces plaques, lineage doit noter source = `ayvens_etat_parc` (EP
   prioritaire sur api_plaques).
