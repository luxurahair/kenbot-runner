# Sprint 2 — PDSF / Rabais / Photos / FB Format

Date: 2026-04-22
Repo cible: **luxurahair/kenbot-runner** (Render cron `kenbot-runner`)

## 📦 Fichiers à remplacer

1. `runner_cron_prod.py`  (bloc storytelling PDSF + disclaimer + lien fiche)
2. `kennebec_scrape.py`   (extraction double prix + fix bug no_photo)
3. `llm_v3.py`            (aucun changement, inclus pour parité)
4. `pipeline/cliches.py`  (aucun changement, inclus pour parité)

## ✨ Changements

### Scraper (kennebec_scrape.py)
- Extrait `pdsf_int` = prix "Était" (Neuf uniquement)
- Calcule `rabais_int` = PDSF − prix affiché
- Fix bug photos : accepte désormais `/images/newcar/` (catalog) comme fallback
  quand le dealer n'a pas encore pris les photos réelles → plus de vehicules
  publiés sans aucune photo

### Cron (runner_cron_prod.py)
- Propage `pdsf_int` et `rabais_int` vers Supabase `inventory`
- Fallback automatique si les colonnes n'existent pas encore (upsert retry)
- Nouveau bloc storytelling en tête des posts FB pour véhicules NEUFS avec PDSF:
  ```
  💰 PDSF affiché : 65 662 $
  ✂️ Vous profitez d'un rabais de 8 645 $
  🔥 Il vous revient à seulement 57 017 $
  ```
- Ajoute avant le footer :
  ```
  * Prix conditionnel à votre choix de financement ou rabais.
  🔗 Fiche détaillée : https://www.kennebecdodge.ca/fr/inventaire-neuf/...
  ```

### Compatibilité
- Occasion: aucun changement (format FB identique, pas de PDSF affiché)
- Neuf SANS PDSF sur le site: pas de bloc storytelling, comportement actuel préservé
- Rétrocompatible: si les colonnes Supabase pdsf_int/rabais_int n'existent pas,
  le cron fonctionne quand même (fallback)

## 🚀 Déploiement

1. Ouvre GitHub https://github.com/luxurahair/kenbot-runner
2. Upload et REMPLACE les 4 fichiers (3 à la racine + pipeline/cliches.py)
3. Render détecte le push et redémarre automatiquement le cron
4. Premiers posts avec nouveau format visibles au prochain tick (toutes les 5 min)

