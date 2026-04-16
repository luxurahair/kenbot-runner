# PRD — Kenbot Ecosystem

## Date: 2026-04-12 (Updated: 2026-04-16)

## Projets

### 1. kenbot-runner (Bot FB — Render Cron) - EN PRODUCTION
- Scrape Kennebec > IA > Facebook (toutes les 60 min)
- Detection: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP
- Footer Daniel Giroux + hashtags SEO + lien tinyurl.com/EvaluerMonAuto
- Pipeline AI: 10 styles intro, 43+ cliches interdits
- VIN decode NHTSA + Window Sticker Stellantis

### 2. kenbot-dashboard (Dashboard — Vercel + Render) - EN PRODUCTION
- URL admin: https://kenbot-dashboard-five.vercel.app
- URL client: https://kenbot-dashboard-five.vercel.app/reprise
- Evaluations reprise avec photos, wholesale inline, prix directeur
- Role-based auth (admin, directeur, conseiller) + mdp oublie
- GPT-4o Vision VIN scanning
- Monitoring: /api/health, /api/cron/status, /api/services/status

### 3. Auto-Repair (GitHub Actions) - EN PRODUCTION
- Health check toutes les 5 min
- Auto-repair + rollback + Vercel deploy hook + email alerte
- DEPLOY_HOOKS integre depuis variables Render

### 4. Pipeline AI Unifie (pipeline/) - V1.0
- Client OpenAI centralise (singleton)
- Prompts centralises + 10 styles intro
- 43+ cliches interdits
- 80/80 tests

## Completed (2026-04-16)
- Kit Auto-Repair avec DEPLOY_HOOKS Vercel
- Pipeline AI unifie
- Formulaire reprise ameliore (VIN label, commentaire etat, barre progression photos, garantie prolongee)
- Wholesale inline dans liste evaluations
- Mot de passe: voir/cacher, oublie (email reset), changement
- Email admin: danielgiroux007@gmail.com dans Supabase
- Endpoints: /api/cron/status, /api/services/status
- README.md complet + kenbot-dashboard/README.md + monitoring/README.md

## Backlog
- P1: Domaine custom (AchatVehiculeQC.ca / reprise.danielgiroux.ca)
- P2: Google Business Profile API
- P2: Collecteur reactions FB
- P3: Decoupage runner_cron_prod.py en modules
- P3: Refactoring App.js en composants separes
- P3: Decoupage server.py en routes modulaires
