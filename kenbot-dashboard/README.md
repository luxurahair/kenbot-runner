# KENBOT DASHBOARD — Centre de Controle & Evaluations

Dashboard web + formulaire client pour Kennebec Dodge Chrysler.
Backend FastAPI sur Render, frontend React sur Vercel.

---

## Architecture

```
kenbot-dashboard/
├── api/                              # Backend FastAPI (Render)
│   ├── server.py                     # API REST complète (~1600 lignes)
│   │   ├── /api/health               # Health check + Supabase status
│   │   ├── /api/cron/status          # Etat dernier cron sync
│   │   ├── /api/services/status      # Etat tous les services
│   │   ├── /api/system/status        # Stats systeme
│   │   ├── /api/inventory            # Inventaire vehicules
│   │   ├── /api/posts                # Posts Facebook
│   │   ├── /api/events               # Journal evenements
│   │   ├── /api/cockpit/data         # Stats cockpit
│   │   ├── /api/evaluations          # CRUD evaluations reprise
│   │   ├── /api/vin/scan-photo       # GPT-4o Vision VIN scan
│   │   ├── /api/wholesale-contacts   # CRUD contacts grossistes
│   │   ├── /api/wholesale/send       # Envoi email wholesale
│   │   ├── /api/reprise/auth/login   # Authentification
│   │   ├── /api/users                # Gestion utilisateurs
│   │   ├── /api/users/change-password
│   │   └── /api/users/forgot-password
│   ├── requirements.txt
│   └── start.py
│
├── frontend/                         # Frontend React (Vercel)
│   ├── src/
│   │   ├── App.js                    # Application (~2300 lignes)
│   │   │   ├── LoginPage             # Login + voir mdp + oublie
│   │   │   ├── Header                # Nav + changement mdp
│   │   │   ├── CockpitTab            # Stats temps reel
│   │   │   ├── InventoryTab          # Liste inventaire
│   │   │   ├── PostsTab              # Posts Facebook
│   │   │   ├── EventsTab             # Journal evenements
│   │   │   ├── EvaluationsTab        # Evaluations + wholesale inline
│   │   │   ├── UsersTab              # Gestion utilisateurs
│   │   │   └── RepriseForm           # Formulaire client standalone
│   │   └── App.css                   # Styles dark theme Torque
│   ├── public/
│   │   ├── index.html
│   │   └── favicon.png               # Logo Kennebec
│   └── package.json
│
├── vercel.json                       # Config Vercel (dans frontend/)
├── render.yaml                       # Config Render
└── SQL Migrations
    ├── supabase_users_migration.sql
    ├── supabase_prix_migration.sql
    └── supabase_wholesale_migration.sql
```

---

## URLs

| Service | URL |
|---------|-----|
| Frontend (admin) | https://kenbot-dashboard-five.vercel.app |
| Formulaire client | https://kenbot-dashboard-five.vercel.app/reprise |
| Lien court reprise | tinyurl.com/EvaluerMonAuto |
| API Backend | https://kenbot-dashboard-api.onrender.com |

---

## Deploiement

### Backend (Render)
- **Plateforme**: Render Web Service
- **Root directory**: `kenbot-dashboard/api`
- **Build**: `pip install -r requirements.txt`
- **Start**: `uvicorn server:app --host 0.0.0.0 --port $PORT`
- **Auto-deploy**: Oui (sur push main)

### Frontend (Vercel)
- **Plateforme**: Vercel
- **Root directory**: `kenbot-dashboard/frontend`
- **Framework**: Create React App
- **Install**: `npm install --legacy-peer-deps` (via vercel.json racine)
- **Auto-deploy**: Via Deploy Hook (`DEPLOY_HOOKS` dans Render env)
- **Note**: Les push par emergent-agent sont bloques, le deploy hook contourne ce probleme

---

## Variables d'Environnement

### Render (kenbot-dashboard-api)

| Variable | Valeur | Description |
|----------|--------|-------------|
| `SUPABASE_URL` | `https://xjhqkh...supabase.co` | URL Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | `eyJhbG...` | Cle service_role |
| `OPENAI_API_KEY` | `sk-proj-...` | GPT-4o (texte + Vision VIN) |
| `EMERGENT_LLM_KEY` | `...` | Cle universelle Emergent (backup) |
| `CORS_ORIGINS` | `*` | Origines CORS |
| `SMTP_HOST` | `smtp.gmail.com` | Serveur SMTP |
| `SMTP_PORT` | `587` | Port SMTP |
| `SMTP_USER` | `info@luxuradistribution.com` | Email expediteur |
| `SMTP_PASS` | `(app password)` | App password Gmail |
| `DEPLOY_HOOKS` | `https://api.vercel.com/v1/...` | Vercel Deploy Hook |

### Vercel (kenbot-dashboard frontend)

| Variable | Valeur |
|----------|--------|
| `REACT_APP_BACKEND_URL` | `https://kenbot-dashboard-api.onrender.com` |

---

## Fonctionnalites Detaillees

### Evaluations (Reprise)
- Formulaire client 7 etapes (standalone /reprise)
- VIN scan par camera (GPT-4o Vision) ou saisie manuelle
- Decode VIN automatique via NHTSA
- Upload photos avec compression + barre de progression
- Statuts: NOUVEAU → EN EVALUATION → OFFRE ENVOYEE → ACCEPTE/REFUSE
- Prix reprise par le directeur
- Wholesale: envoi par email aux grossistes avec photos (reply-to directeur)
- Miniatures photos + wholesale inline dans la liste

### Authentification
- Login/password avec toggle voir/cacher
- "Mot de passe oublie" → email avec mdp temporaire
- Changement mdp dans le header (tous les users)
- Admin: CRUD complet utilisateurs avec email

### Monitoring
- `/api/health` — Sante API + Supabase
- `/api/cron/status` — Dernier run, 5 derniers events, total runs
- `/api/services/status` — Supabase, API, Cron, SMTP, Vercel (tout d'un coup)

---

## Comptes

| Username | Role | Email |
|----------|------|-------|
| admin | admin | danielgiroux007@gmail.com |
| directeur | directeur | (a configurer) |

---

*Kennebec Dodge Chrysler — 10240 boul. Lacroix, Saint-Georges — 418-222-3939*
