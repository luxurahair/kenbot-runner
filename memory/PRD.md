# PRD — Kenbot Ecosystem

## Date: 2026-04-12 (Updated: 2026-04-16)

## Projets

### 1. kenbot-runner (Bot FB — Render Cron) - EN PRODUCTION
- Scrape Kennebec > IA > Facebook
- Detection par STOCK: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP
- Footer pro Daniel Giroux + hashtags SEO dynamiques
- Nettoyeur post-IA (_clean_ai_output)
- PROFIL DU VEHICULE + CARACTERISTIQUES CERTIFIEES

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
- Tests: 42/43 (seul Lamborghini Huracan manquant dans vehicle_intelligence.py)
- Compatibilite arriere: TOUS les imports existants fonctionnent

## Pipeline Architecture
```
pipeline/
├── __init__.py          # Module description
├── client.py            # Client OpenAI centralise (singleton)
├── prompts.py           # Prompts/system messages centralises
├── generator.py         # Fonctions generation unifiees
└── cliches.py           # Filtre anti-cliches (source unique)
```

## Events du Cron
- NEW: Nouveau vehicule
- PRICE_CHANGED: Baisse de prix
- PHOTOS_ADDED: Photos ajoutees (remplace NO_PHOTO)
- SOLD: Vehicule vendu (prefixe VENDU)
- UNSOLD: Faux vendu corrige (restauration)

## Completed (2026-04-16)
- Kit Auto-Repair pour Kenbot: monitoring/auto_repair.py + .github/workflows/auto-repair.yml
- Health endpoint /api/health ajoute au backend
- SMTP notifications testees et fonctionnelles
- Pipeline OpenAI unifie: pipeline/ module cree
- Programme de tests complet: tests/test_pipeline_generation.py
- Backup complet: backup_2026-04-16/ (9 fichiers)

## Backlog
- P0: Deployer via "Save to Github" + ajouter secrets GitHub (SMTP_USER, SMTP_PASS)
- P1: Domaine custom (AchatVehiculeQC.ca ou reprise.danielgiroux.ca)
- P2: Google Business Profile API
- P2: Collecteur reactions FB
- P3: Decoupage runner_cron_prod.py en modules
- P3: Refactoring App.js (>1800 lignes) en composants separes
- P3: Decoupage server.py en routes modulaires
