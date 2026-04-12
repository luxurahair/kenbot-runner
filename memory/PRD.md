# PRD — Kenbot Ecosystem

## Date: 2026-04-12

## Projets

### 1. kenbot-runner (Bot FB — Render Cron)
- Statut: EN PRODUCTION
- Scrape Kennebec → IA → Facebook
- Détection par STOCK: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP
- Footer pro Daniel Giroux + hashtags SEO dynamiques

### 2. kenbot-dashboard (Dashboard — Vercel + Render)
- Statut: EN PRODUCTION
- URL: https://kenbot-dashboard-five.vercel.app
- Cockpit, Inventaire, Posts FB, Events, Architecture, Changelog

### 3. kenbot-reprise (App Reprise — À CONSTRUIRE)
- Statut: PLANIFIÉ
- Formulaire public: VIN décodage auto + photos + infos client
- Dashboard admin: liste évaluations + fiches détaillées + statuts
- Stack: React + FastAPI + Supabase + Vercel + Render
- Login admin: 418-222-3939 / Liana2018$

## Backlog
- P0: Construire kenbot-reprise
- P0: Pousser kenbot-dashboard mis à jour sur GitHub (onglet Kennebec vs FB)
- P1: Phase A — Pipeline OpenAI unifié
- P1: Multi-dealer Luxura
- P2: Google Business Profile API
- P2: Collecteur réactions FB
- P2: Domaine custom reprise.danielgiroux.ca
- P3: Découpage runner_cron_prod.py en modules
