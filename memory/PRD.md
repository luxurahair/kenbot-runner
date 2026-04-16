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

### 3. kenbot-reprise (App Reprise) - MERGED INTO kenbot-dashboard
- V2 integree dans kenbot-dashboard
- Formulaire client standalone: /reprise
- Admin dashboard avec login, evaluations, filtres, vue detail
- Gestion des statuts: NOUVEAU, EN EVALUATION, OFFRE ENVOYEE, ACCEPTE, REFUSE

## Completed (2026-04-16)
- Kit Auto-Repair pour Kenbot: monitoring/auto_repair.py + .github/workflows/auto-repair.yml
- Health endpoint /api/health ajoute au backend
- SMTP notifications testees et fonctionnelles (port 587 TLS)
- Cron GitHub Actions toutes les 5 minutes
- Auto-rollback et notifications email en cas de panne

## Backlog
- P0: Deployer sur Render (health endpoint + wholesale) via "Save to Github"
- P0: Ajouter secrets GitHub (SMTP_USER, SMTP_PASS) pour activer les notifications
- P1: Domaine custom (AchatVehiculeQC.ca ou reprise.danielgiroux.ca)
- P1: Phase A — Pipeline OpenAI unifie (refactoring llm.py, llm_v3.py, sticker_to_ad.py)
- P2: Google Business Profile API
- P2: Collecteur reactions FB
- P3: Decoupage runner_cron_prod.py en modules
- P3: Refactoring App.js (>1800 lignes) en composants separes
- P3: Decoupage server.py en routes modulaires
