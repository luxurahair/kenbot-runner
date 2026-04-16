# KENEBEC-AI — Ecosysteme Complet Kennebec Dodge Chrysler

Plateforme intelligente pour la gestion automatisee de l'inventaire, la publication Facebook, les evaluations de reprise et le monitoring — Kennebec Dodge Chrysler, Saint-Georges, Beauce.

---

## Architecture Globale

```
kenebec-ai/
│
├── CRON BOT (Render Cron — kenbot-runner)
│   ├── runner_cron_prod.py         # Orchestrateur principal (~1800 lignes)
│   ├── kennebec_scrape.py          # Scraper HTML inventaire + extraction VIN
│   ├── vin_decoder.py              # Decodage VIN via NHTSA API
│   ├── vehicle_intelligence.py     # 27+ marques, 43+ modeles, 194+ trims
│   ├── sticker_to_ad.py            # Extraction PDF Window Sticker Stellantis
│   ├── ad_builder.py               # Construction annonce structuree
│   ├── footer_utils.py             # Footer Daniel Giroux + hashtags SEO + lien reprise
│   ├── fb_api.py                   # Facebook Graph API wrapper
│   ├── supabase_db.py              # Supabase PostgreSQL wrapper
│   ├── meta_compare_supabase.py    # Audit site vs Facebook (rapport CSV)
│   ├── classifier.py               # Classification type vehicule
│   ├── llm.py                      # Generation AI v2 (accroches courtes)
│   ├── llm_v3.py                   # Generation AI v3 (annonces completes)
│   ├── text_engine_client.py       # Service kdc-dgtext externe (fallback)
│   └── dg_text.py                  # Templates texte statiques
│
├── PIPELINE AI UNIFIE (pipeline/)
│   ├── __init__.py                 # Module description
│   ├── client.py                   # Client OpenAI centralise (singleton)
│   ├── prompts.py                  # Prompts/system messages centralises
│   ├── generator.py                # Fonctions generation unifiees
│   └── cliches.py                  # Filtre anti-cliches (43+ expressions)
│
├── DASHBOARD WEB (kenbot-dashboard/)
│   ├── api/                        # Backend FastAPI (Render)
│   │   ├── server.py               # API REST (~1600 lignes)
│   │   ├── requirements.txt
│   │   └── start.py
│   ├── frontend/                   # Frontend React (Vercel)
│   │   ├── src/App.js              # Application principale
│   │   ├── src/App.css             # Styles dark theme
│   │   └── public/
│   └── render.yaml
│
├── MONITORING & AUTO-REPAIR (monitoring/)
│   ├── auto_repair.py              # Script monitoring + reparation auto
│   └── README.md                   # Guide installation
│
├── GITHUB ACTIONS (.github/workflows/)
│   └── auto-repair.yml             # Cron 5 min: health check + repair + email
│
├── TESTS (tests/)
│   ├── test_pipeline_generation.py # 80+ tests (AI, VIN, cliches, footer, events)
│   ├── test_pipeline_complet.py    # 88 tests pipeline bout-en-bout
│   ├── test_sold_unsold_logic.py   # 11 tests logique SOLD/UNSOLD
│   ├── test_cliches.py             # Tests filtre anti-cliches
│   ├── test_footer_detection.py    # Tests footer Daniel Giroux
│   └── test_text_generation.py     # Tests generation texte
│
├── OUTILS (tools/)
│   ├── audit_and_fix_live.py       # Audit et correction posts FB live
│   ├── audit_meta_feed_vs_posts.py # Comparaison meta feed vs posts
│   ├── audit_sold_ghosts.py        # Detection posts fantomes vendus
│   ├── bulk_update_fb_text_from_stickers.py
│   ├── dedup_inventory_by_stock_delete.py
│   ├── dedup_posts_by_stock_delete.py
│   └── cleanup_*.py                # Scripts de nettoyage Supabase
│
├── BACKUP (backup_2026-04-16/)     # Sauvegarde pre-refactoring pipeline
│
├── vercel.json                     # Config Vercel racine (--legacy-peer-deps)
├── requirements.txt                # Dependances Python cron bot
├── runtime.txt                     # Python 3.11.x
└── README.md                       # Ce fichier
```

---

## Services & Hebergement

| Service | Plateforme | URL | Role |
|---------|-----------|-----|------|
| **kenbot-runner** | Render Cron | (interne) | Bot scrape + FB ads toutes les 60 min |
| **kenbot-dashboard-api** | Render Web Service | `https://kenbot-dashboard-api.onrender.com` | API REST backend |
| **kenbot-dashboard** | Vercel | `https://kenbot-dashboard-five.vercel.app` | Frontend React |
| **Supabase** | Supabase.co | `https://xjhqkhlocxtawiuokrlp.supabase.co` | PostgreSQL + Storage |
| **Auto-Repair** | GitHub Actions | (repo kenebec-ai) | Monitoring 5 min + auto-repair |

---

## Pipeline Cron (toutes les 60 min)

```
1. SCRAPE kennebecdodge.ca (3 pages) → ~47 vehicules
2. SCRAPE_RUN cree dans Supabase (FK pour sticker_pdfs)
3. PRE-CACHE PDFs Stellantis 2018+ (cache hit ~100%)
4. INDEX par STOCK (source de verite)
5. DETECTION:
   ├── UNSOLD       — Post marque VENDU mais stock encore sur Kennebec → restaurer
   ├── PHOTOS_ADDED — FB a 0-1 photo ET Kennebec > 1 → delete + recreate
   ├── PRICE_CHANGED— Prix different > 200$ → update texte + intro rabais
   ├── NEW          — Slug pas dans inv_db → nouveau post
   ├── SOLD         — Stock PAS sur Kennebec + cooldown 3 jours → marquer VENDU
   └── CLEANUP      — Corriger double footer (max 10/run)
6. RAPPORT meta_vs_site.csv uploade dans Supabase Storage
```

## Generation de Texte AI

```
Priorite 1: Stellantis + Window Sticker + AI humanisation
   sticker_to_ad.py → extraction options PDF
   llm_v3.py / pipeline/generator.py → generate_smart_text
   vehicle_intelligence.py → contexte vehicule

Priorite 2: LLM_V3 (tous vehicules, avec VIN NHTSA)
   vin_decoder.py → moteur, HP, transmission, 4WD
   10 styles d'intro: nouvelle arrivage, question client, chiffre punch,
   rarete, prix/deal, pour qui, spec technique, histoire, saison, le char parle

Priorite 3: Sticker brut (sans AI)
   sticker_to_ad.py → build_ad
   llm.py → generate_intro_only

Priorite 4: text_engine_client (service kdc-dgtext externe)

Footer: footer_utils.py (source unique)
   - Daniel Giroux 418-222-3939
   - Echanges (auto, moto, bateau, VTT, cote-a-cote)
   - Lien reprise: tinyurl.com/EvaluerMonAuto
   - Hashtags SEO dynamiques (#DodgeHornet2024 #Beauce #Pickup)

Filtre: 43+ cliches interdits (pipeline/cliches.py)
   - Jamais "passionne depuis 20 ans", "sillonner les routes", "faire tourner les tetes"
   - Toujours commencer par LE VEHICULE, jamais le vendeur
```

---

## Dashboard Web (kenbot-dashboard)

### Fonctionnalites

| Module | Role | Acces |
|--------|------|-------|
| **Cockpit** | Stats live: actifs, vendus, no_photo, audit prix | admin |
| **Inventaire** | Liste vehicules scrapes (VIN, prix, km) | admin, directeur |
| **Posts FB** | Posts Facebook: ACTIVE, SOLD, photo_count | admin |
| **Events** | Journal (NEW, SOLD, UNSOLD, PHOTOS_ADDED) | admin |
| **Evaluations** | Demandes reprise: photos, wholesale, prix | admin, directeur, conseiller |
| **Wholesale** | Contacts grossistes + envoi email avec photos | admin, directeur |
| **Utilisateurs** | Gestion comptes (CRUD, email, mot de passe) | admin |
| **Architecture** | Diagramme composants et flux | admin |
| **Changelog** | Historique versions | admin |

### Roles

| Role | Acces |
|------|-------|
| `admin` | Tout: cockpit, inventaire, posts, events, evaluations, wholesale, users |
| `directeur` | Evaluations (prix reprise, wholesale), changement mot de passe |
| `conseiller` | Evaluations (lecture + creation), changement mot de passe |

### Formulaire Client (/reprise)

Formulaire standalone accessible sans login pour les clients:

| Etape | Contenu |
|-------|---------|
| 1. Coordonnees | Nom, telephone, courriel, projet (achat/echange/location) |
| 2. Vehicule | VIN (saisie manuelle ou scan camera), decode NHTSA, km, couleurs |
| 3. Equipements | Checkboxes: confort, technologie, securite, performance, exterieur |
| 4. Etat | Barre 5 niveaux + pare-brise + dommages visibles + commentaire libre |
| 5. Photos | Upload avec barre de progression (compression auto, max 10) |
| 6. Garanties | Fabricant (date exp.) + prolongee (date, fournisseur, details) |
| 7. Commentaires | Zone texte libre |

### Authentification

- Login avec mot de passe
- Bouton "Voir/Cacher" mot de passe
- "Mot de passe oublie" → envoie un mdp temporaire par email
- Changement de mot de passe (tous les users, dans le header)

---

## API Endpoints

### Systeme
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Sante API + Supabase |
| GET | `/api/system/status` | Stats completes |
| GET | `/api/cron/status` | Etat dernier cron (run, events) |
| GET | `/api/services/status` | Etat tous les services (Supabase, API, Cron, SMTP, Vercel) |

### Donnees
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/inventory` | Inventaire complet |
| GET | `/api/posts` | Posts Facebook |
| GET | `/api/events` | Journal d'evenements |
| GET | `/api/cockpit/data` | Stats cockpit |

### Evaluations (Reprise)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/evaluations` | Creer une evaluation |
| GET | `/api/evaluations` | Lister les evaluations |
| GET | `/api/evaluations/{id}` | Detail evaluation |
| PATCH | `/api/evaluations/{id}` | Modifier (status, prix_reprise) |
| POST | `/api/evaluations/upload-photo` | Upload photo Supabase |
| POST | `/api/vin/scan-photo` | Scan VIN par photo (GPT-4o Vision) |

### Wholesale
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/wholesale-contacts` | Lister contacts |
| POST | `/api/wholesale-contacts` | Ajouter contact |
| PATCH | `/api/wholesale-contacts/{id}` | Modifier contact |
| POST | `/api/wholesale/send` | Envoyer evaluation par email (reply-to directeur) |

### Utilisateurs
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/reprise/auth/login` | Connexion |
| GET | `/api/users` | Lister (admin) |
| POST | `/api/users` | Creer utilisateur |
| PATCH | `/api/users/{username}` | Modifier (nom, email, role, mdp) |
| POST | `/api/users/change-password` | Changer mot de passe (tous) |
| POST | `/api/users/forgot-password` | Reinitialiser par email |

---

## Monitoring & Auto-Repair

### Fonctionnement (GitHub Actions — toutes les 5 minutes)

```
TOUTES LES 5 MINUTES
         │
         v
  ┌───────────────┐
  │  Health Check  │ → /api/health, /api/evaluations, /api/cron/status,
  │  5 endpoints   │   /api/services/status, /api/wholesale-contacts
  └───────┬───────┘
          │
    ┌─────┴──────┬────────┐
    │            │         │
    v            v         v
 HEALTHY    DEGRADED     DOWN
    │            │         │
    v            │         v
 Save Stable    │    AUTO-REPAIR
 Commit         │    1. Analyze erreur
                │    2. Trigger Vercel deploy hook
                │    3. Rollback ou restart Render
                │    4. Email notification
                │         │
                v         v
         Email si probleme
```

### Alertes Email
- **Destinataire**: info@luxuradistribution.com (SMTP Gmail)
- **Quand**: Service DOWN ou DEGRADED
- **Contenu**: Status, actions de reparation, lien vers le workflow

---

## Variables d'Environnement

### Render — kenbot-dashboard-api

| Variable | Obligatoire | Description |
|----------|-------------|-------------|
| `SUPABASE_URL` | Oui | URL projet Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Oui | Cle service_role Supabase |
| `OPENAI_API_KEY` | Oui | Cle API OpenAI (GPT-4o Vision + generation) |
| `EMERGENT_LLM_KEY` | Non | Cle universelle Emergent (backup GPT-4o) |
| `CORS_ORIGINS` | Non | Origines CORS (defaut: `*`) |
| `SMTP_HOST` | Oui | `smtp.gmail.com` |
| `SMTP_PORT` | Oui | `587` |
| `SMTP_USER` | Oui | `info@luxuradistribution.com` |
| `SMTP_PASS` | Oui | App password Gmail |
| `DEPLOY_HOOKS` | Non | URL Vercel Deploy Hook (auto-deploy) |

### Render — kenbot-runner (Cron)

| Variable | Obligatoire | Description |
|----------|-------------|-------------|
| `SUPABASE_URL` | Oui | URL projet Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Oui | Cle service_role Supabase |
| `KENBOT_FB_PAGE_ID` | Oui | ID page Facebook |
| `KENBOT_FB_ACCESS_TOKEN` | Oui | Token Facebook (permanent) |
| `OPENAI_API_KEY` | Oui | Cle API OpenAI |
| `KENBOT_BASE_URL` | Non | `https://www.kennebecdodge.ca` |
| `KENBOT_INVENTORY_PATH` | Non | `/fr/inventaire-occasion/` |
| `KENBOT_MAX_TARGETS` | Non | `25` — Max vehicules par run |
| `KENBOT_SLEEP_BETWEEN` | Non | `3` — Secondes entre publications |
| `KENBOT_POST_COOLDOWN_DAYS` | Non | `7` — Jours avant re-publication |
| `KENBOT_PRICE_CHANGE_THRESHOLD` | Non | `200` — Seuil changement prix ($) |
| `KENBOT_REFRESH_NO_PHOTO_DAILY` | Non | `true` — Detection PHOTOS_ADDED |
| `KENBOT_REFRESH_NO_PHOTO_LIMIT` | Non | `25` — Limite par run |
| `KENBOT_USE_AI` | Non | `true` — Utiliser GPT-4o |
| `KENBOT_USE_STICKER_AD` | Non | `true` — Utiliser Window Stickers |
| `KENBOT_STICKERS_BUCKET` | Non | `kennebec-stickers` |
| `USE_HUMANIZE` | Non | `true` — Humaniser stickers |

### GitHub Actions — auto-repair.yml

| Variable | Source | Description |
|----------|--------|-------------|
| `SERVICE_URL` | env workflow | `https://kenbot-dashboard-api.onrender.com` |
| `SMTP_HOST/PORT/USER/PASS` | env workflow | Config email alertes |
| `DEPLOY_HOOKS` | env workflow | Vercel Deploy Hook URL |
| `GITHUB_TOKEN` | auto GitHub | Token pour push (auto-fourni) |

### Vercel — kenbot-dashboard frontend

| Variable | Description |
|----------|-------------|
| `REACT_APP_BACKEND_URL` | URL backend Render |

---

## Base de Donnees Supabase

### Tables

| Table | PK | Description |
|-------|-----|-------------|
| `inventory` | slug | Inventaire scrape (stock, vin, prix, km, status, photos) |
| `posts` | slug | Posts Facebook (post_id, status, base_text, photo_count) |
| `events` | id | Journal d'evenements (NEW, SOLD, UNSOLD, etc.) |
| `scrape_runs` | run_id | Historique des runs cron |
| `sticker_pdfs` | vin | Cache PDFs Window Sticker (status, storage_path) |
| `evaluations` | id | Demandes de reprise (client, vehicule, photos, prix) |
| `dashboard_users` | id | Utilisateurs dashboard (username, password, role, email) |
| `wholesale_contacts` | id | Contacts grossistes (name, email, phone, active) |

### Storage Buckets

| Bucket | Contenu |
|--------|---------|
| `kennebec-stickers` | PDFs Window Sticker valides |
| `reprise-photos` | Photos evaluations client (public) |
| `kennebec-outputs` | Rapports CSV meta vs site |

---

## Tests

```bash
# Pipeline generation complet (80+ tests — AI, VIN, cliches, footer, events)
python tests/test_pipeline_generation.py              # Avec API OpenAI
python tests/test_pipeline_generation.py --no-api     # Sans API (parsing seulement)

# Pipeline bout-en-bout (88 tests)
python tests/test_pipeline_complet.py
python tests/test_pipeline_complet.py --with-ai

# Logique SOLD / UNSOLD / PRICE_CHANGED
python tests/test_sold_unsold_logic.py                # 11 tests

# Filtre anti-cliches
python tests/test_cliches.py

# Footer Daniel Giroux
python tests/test_footer_detection.py
```

---

## Comptes Utilisateurs

| Username | Role | Email | Acces |
|----------|------|-------|-------|
| `admin` | admin | danielgiroux007@gmail.com | Tout |
| `directeur` | directeur | (a configurer) | Evaluations + wholesale |
| Conseillers | conseiller | (a creer) | Evaluations |

---

## Historique des Versions

| Version | Date | Changements |
|---------|------|-------------|
| 4.2.0 | 2026-04-16 | Pipeline AI unifie, auto-repair, formulaire reprise ameliore, wholesale inline, mdp oublie |
| 4.1.0 | 2026-04-15 | Torque UI, role-based auth, GPT-4o VIN scan, Supabase photos, wholesale |
| 4.0.0 | 2026-04-12 | UNSOLD, SEO hashtags, cleanup footer, comparaison par STOCK |

---

*Kennebec Dodge Chrysler — 10240 boul. Lacroix, Saint-Georges — 418-222-3939*
