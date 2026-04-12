# PRD — Kenbot Ecosystem

## Date: 2026-04-12

## Projets

### 1. kenbot-runner (Bot FB — Render Cron) ✅ EN PRODUCTION
- Scrape Kennebec → IA → Facebook
- Détection par STOCK: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP
- Footer pro Daniel Giroux + hashtags SEO dynamiques
- Nettoyeur post-IA (_clean_ai_output)
- PROFIL DU VÉHICULE + CARACTÉRISTIQUES CERTIFIÉES (pas NHTSA/SPECS VIN)

### 2. kenbot-dashboard (Dashboard — Vercel + Render) ✅ EN PRODUCTION
- URL: https://kenbot-dashboard-five.vercel.app
- Cockpit, Inventaire, Posts FB, Events, Architecture, Changelog
- À POUSSER: onglet Kennebec vs FB (CompareTab)

### 3. kenbot-reprise (App Reprise — V1 CRÉÉE, V2 À FAIRE)
- V1: Formulaire basique VIN+photos + admin
- V2 TODO: Reproduire le design Torque complet
  - Sidebar navigation (Évaluations, Inventaire, Pneus, Ventes, Clients)
  - Formulaire multi-sections: Client, Véhicule (VIN auto), Options checkboxes, État véhicule (barre couleur MAUVAIS→EXCELLENT), Pare-brise, Garanties
  - Liste évaluations avec tableau, filtres (Toutes/Complété/En attente/Perdu/Repris), recherche VIN/client
  - Actions: imprimer, partager, modifier
  - Boutons: Annuler, Sauvegarder brouillon, Sauvegarder et quitter
  - Logos marques dans la liste
- Login admin: 418-222-3939 / Daniel7$
- Stack: React + FastAPI + Supabase + Vercel + Render

## Référence Torque (screenshots capturés)
- Login page: dark theme, logo, phone+password
- Nouvelle évaluation: formulaire multi-sections avec nav sidebar
- Sections: Client, Véhicule (VIN décodage), Options, Commentaires, État (barre couleur), Pare-brise, Garanties
- Liste: tableau Date/Véhicule/Client/Valeurs/Statut + filtres + pagination
- 32 évaluations existantes chez Kennebec dans Torque

## Backlog
- P0: kenbot-reprise v2 (design Torque complet)
- P0: Pousser kenbot-dashboard GitHub (CompareTab)
- P1: Phase A — Pipeline OpenAI unifié
- P1: Multi-dealer Luxura
- P2: Google Business Profile API
- P2: Collecteur réactions FB
- P3: Découpage runner_cron_prod.py en modules
