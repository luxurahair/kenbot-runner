# KENBOT REPRISE — App de Reprise / Évaluation de Véhicules

Application permettant aux clients d'envoyer les informations de leur véhicule pour une évaluation de reprise par Daniel Giroux, Kennebec Dodge Chrysler.

## Fonctionnalités

### Formulaire Public (`/evaluer`)
- Décodage VIN automatique (NHTSA API)
- Upload photos (max 10, Supabase Storage)
- Informations: km, paiement restant, état général
- Coordonnées du client

### Dashboard Admin (`/admin`)
- Login sécurisé (téléphone + mot de passe)
- Liste des évaluations avec filtres par statut
- Fiche détaillée: VIN décodé, photos, infos client
- Statuts: NOUVEAU → EN ÉVALUATION → OFFRE ENVOYÉE → ACCEPTÉ/REFUSÉ

## Stack
- **Frontend**: React 18 + React Router (Vercel)
- **Backend**: FastAPI + Supabase (Render)
- **Base de données**: Supabase PostgreSQL
- **Storage**: Supabase Storage (photos)
- **VIN**: NHTSA vPIC API (gratuit, pas de clé)

## Déploiement

### Backend (Render)
1. Web Service → Root: `api` → Python
2. Build: `pip install -r requirements.txt`
3. Start: `uvicorn server:app --host 0.0.0.0 --port $PORT`

### Frontend (Vercel)
1. Root Directory: `frontend`
2. Build: `npm run build`
3. Variable: `REACT_APP_BACKEND_URL` = URL Render

### Variables Render
| Variable | Description |
|---|---|
| `SUPABASE_URL` | URL Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Clé service_role |
| `ADMIN_PHONE` | 4182223939 |
| `ADMIN_PASSWORD` | Mot de passe admin |

## Table Supabase

```sql
CREATE TABLE evaluations (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    status TEXT DEFAULT 'NOUVEAU',
    client_name TEXT,
    client_phone TEXT,
    client_email TEXT,
    client_notes TEXT,
    vin TEXT,
    make TEXT,
    model TEXT,
    year TEXT,
    trim TEXT,
    engine TEXT,
    drive_type TEXT,
    fuel_type TEXT,
    km INTEGER,
    paiement_restant NUMERIC,
    etat_general TEXT,
    photos JSONB DEFAULT '[]',
    vin_decoded JSONB DEFAULT '{}',
    admin_notes TEXT,
    offre_montant NUMERIC
);
```
