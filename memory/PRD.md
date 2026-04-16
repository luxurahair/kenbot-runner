# PRD — Kenbot Ecosystem

## Date: 2026-04-12 (Updated: 2026-04-16)

## Projets

### 1. kenbot-runner (Bot FB — Render Cron) - EN PRODUCTION
- Scrape Kennebec > IA > Facebook
- Detection par STOCK: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP
- Footer pro Daniel Giroux + hashtags SEO dynamiques + lien tinyurl.com/EvaluerMonAuto
- Nettoyeur post-IA (_clean_ai_output)
- PROFIL DU VEHICULE + CARACTERISTIQUES CERTIFIEES
- Meta compare (site vs FB) en fin de run

### 2. kenbot-dashboard (Dashboard — Vercel + Render) - EN PRODUCTION
- URL: https://kenbot-dashboard-five.vercel.app
- Cockpit, Inventaire, Posts FB, Events, Architecture, Changelog
- Reprise/Evaluations style Torque Management integrees
- Role-based auth (admin, directeur, conseiller)
- GPT-4o Vision VIN scanning
- Supabase Storage (reprise-photos bucket)
- Wholesale contacts + email sending
- Auto-repair monitoring system (GitHub Actions)

### 3. Pipeline OpenAI Unifie - V1.0 COMPLETE
- Module centralise: pipeline/ (client.py, prompts.py, generator.py, cliches.py)
- ZERO modification a runner_cron_prod.py
- Backup complet: backup_2026-04-16/
- Tests: 80/80 (avec API OpenAI)
- 10 styles d'intro differents (nouvelle arrivage, question, chiffre, rarete, prix, etc.)
- Interdiction stricte de commencer par experience/passion vendeur
- 43+ cliches interdits (dont "faire tourner les tetes", "passionne depuis...")
- Lien reprise: tinyurl.com/EvaluerMonAuto

## Events du Cron
- NEW: Nouveau vehicule
- PRICE_CHANGED: Baisse de prix
- PHOTOS_ADDED: Photos ajoutees (remplace NO_PHOTO)
- SOLD: Vehicule vendu (prefixe VENDU)
- UNSOLD: Faux vendu corrige (restauration)

## Completed (2026-04-16)
- Kit Auto-Repair: monitoring/auto_repair.py + .github/workflows/auto-repair.yml
- Health endpoint /api/health ajoute au backend
- SMTP notifications testees et fonctionnelles
- Pipeline OpenAI unifie: pipeline/ module
- Tests complets 80/80: tests/test_pipeline_generation.py
- Backup complet: backup_2026-04-16/ (9 fichiers + STRUCTURE.md)
- Lamborghini Huracan/Urus ajoutes a vehicle_intelligence.py
- Intros variees et centrees sur le vehicule (plus de "passionne depuis 20 ans")
- Lien reprise tinyurl.com/EvaluerMonAuto dans le footer

## Backlog
- P0: Deployer via "Save to Github" + ajouter secrets GitHub (SMTP_USER, SMTP_PASS)
- P1: Domaine custom (AchatVehiculeQC.ca ou reprise.danielgiroux.ca)
- P2: Google Business Profile API
- P2: Collecteur reactions FB
- P3: Decoupage runner_cron_prod.py en modules
- P3: Refactoring App.js (>1800 lignes) en composants separes
- P3: Decoupage server.py en routes modulaires
