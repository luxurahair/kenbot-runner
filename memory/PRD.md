# PRD — Kenbot Dashboard + Bot Intelligence

## Date: 2026-04-12

## Ce qui a été fait

### Sessions 1-11 (résumé)
- Dashboard React + FastAPI connecté Supabase live
- Intelligence véhicule (27 marques, 43 modèles, 194 trims)
- llm_v3.py (GPT-4o, Daniel Giroux, 5 styles d'intro)
- VIN decoder NHTSA (vin_decoder.py)
- Cockpit (Sync Status, Audit Prix, Marquer Vendus)
- Détection VENDU/SOLD dans le cron
- Séparation kenbot-dashboard standalone (Render + Vercel)

### Session 12 (2026-04-12)
- FIX: publish_with_photos → publish_photos_unpublished + create_post_with_attached_media
- FIX: Duplicate key slug → on_conflict="slug" + fallback 3 niveaux
- FIX: Double _build_ad_text → réutilise msg (÷2 appels OpenAI)
- FIX: FK sticker_pdfs → upsert_scrape_run AVANT pré-cache
- FIX: Photos commentaires 403 → complètement supprimé (max 10 photos/post)
- FIX: Double footer → ad_builder.py ne rajoute plus les échanges
- AJOUT: Toutes comparaisons par STOCK (pas slug): PRICE_CHANGED, PHOTOS_ADDED, SOLD
- AJOUT: UNSOLD — restaure les faux VENDU si stock encore sur Kennebec
- AJOUT: Cleanup auto double footer (max 10/run)
- AJOUT: Footer pro Daniel Giroux (conseiller expert 20 ans, hashtags SEO dynamiques)
- AJOUT: Plus de lien kennebecdodge.ca dans les annonces
- AJOUT: Prix fallback depuis inventory DB
- AJOUT: Intro PRICE_CHANGED avec montant rabais
- AJOUT: Endpoint /api/vehicles/compare + CompareTab (Kennebec vs FB)
- FIX: Dashboard Vercel live (craco→react-scripts, ajv fix, imports fix)
- Dashboard URL: https://kenbot-dashboard-five.vercel.app

## Architecture
```
kenbot-runner/ (Bot — Render Cron)
├── runner_cron_prod.py (orchestrateur ~1700 lignes)
├── kennebec_scrape.py, vin_decoder.py, vehicle_intelligence.py
├── llm_v3.py, sticker_to_ad.py, ad_builder.py, footer_utils.py
├── fb_api.py, supabase_db.py, meta_compare_supabase.py
└── tests/ (88 + 11 tests)

kenbot-dashboard/ (Dashboard — Vercel + Render)
├── api/server.py (FastAPI)
└── frontend/src/App.js (React)
```

## Backlog
- P0: Vérifier logs cron (UNSOLD, CLEANUP, nouveau footer)
- P0: Pousser App.js/server.py mis à jour sur kenbot-dashboard GitHub
- P1: Phase A — Pipeline OpenAI unifié (JSON structuré)
- P1: Multi-dealer Luxura
- P2: Collecteur réactions FB
- P2: Google Business Profile API
- P2: Alertes/notifications
- P3: Découpage runner_cron_prod.py en modules
