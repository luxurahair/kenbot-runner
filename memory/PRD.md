# PRD — Kenbot Ecosystem

## Date: 2026-04-12 (Updated: 2026-02-01)

## Projets

### 1. kenbot-runner (Bot FB — Render Cron) - EN PRODUCTION
- Scrape Kennebec → IA → Facebook
- Detection par STOCK: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP
- Footer pro Daniel Giroux + hashtags SEO dynamiques
- Nettoyeur post-IA (_clean_ai_output)
- PROFIL DU VEHICULE + CARACTERISTIQUES CERTIFIEES

### 2. kenbot-dashboard (Dashboard — Vercel + Render) - EN PRODUCTION
- URL: https://kenbot-dashboard-five.vercel.app
- Cockpit, Inventaire, Posts FB, Events, Architecture, Changelog
- A POUSSER: onglet Kennebec vs FB (CompareTab)

### 3. kenbot-reprise (App Reprise) - V2 COMPLETE
- **V2 LIVREE**: Design complet style Torque Management
  - Sidebar navigation (Client, Vehicule, Options, Etat, Photos, Garanties, Notes)
  - Formulaire multi-sections avec dark theme professionnel
  - VIN auto-decode via API NHTSA
  - Barre d'etat couleur MAUVAIS → EXCELLENT
  - Options checkboxes par categories (Confort, Technologie, Securite, Performance, Exterieur)
  - Dommages carrosserie (15 zones selectionnables)
  - Upload photos (max 10)
  - Garanties constructeur/prolongee avec toggles
  - Admin dashboard avec login, tableau evaluations, filtres/recherche, vue detail
  - Gestion des statuts: NOUVEAU, EN EVALUATION, OFFRE ENVOYEE, ACCEPTE, REFUSE
- Login admin: 418-222-3939 / Daniel7$
- Stack: React (Tailwind + Shadcn) + FastAPI + Supabase
- **ACTION REQUISE**: Creer la table `evaluations` dans Supabase (voir /app/kenbot-reprise/supabase_migration.sql)

## Tests (2026-02-01)
- Frontend: 100% (toutes les sections UI et interactions fonctionnelles)
- Backend: 100% (16/16 tests passes)
- VIN decode API: Fonctionne (teste avec Jeep Grand Cherokee 2019)
- Admin login: Fonctionne
- Soumission formulaire: API appelee correctement (table Supabase requise)

## Backlog
- P0: Creer table evaluations dans Supabase + tester soumission end-to-end
- P0: Pousser kenbot-dashboard GitHub (CompareTab)
- P1: Phase A — Pipeline OpenAI unifie
- P1: Multi-dealer Luxura
- P2: Google Business Profile API
- P2: Domaine custom reprise.danielgiroux.ca
- P2: Collecteur reactions FB
- P3: Decoupage runner_cron_prod.py en modules
