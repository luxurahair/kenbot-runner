# KENBOT MONITORING — Auto-Repair System

Systeme de surveillance et reparation automatique pour l'ecosysteme Kenbot.
Tourne toutes les 5 minutes via GitHub Actions.

---

## Fonctionnement

```
TOUTES LES 5 MINUTES (GitHub Actions cron)
         │
         v
  ┌───────────────────┐
  │   Health Check     │
  │   5 endpoints:     │
  │   - /api/health    │
  │   - /api/evaluations│
  │   - /api/cron/status│
  │   - /api/services/status│
  │   - /api/wholesale-contacts│
  └─────────┬─────────┘
            │
      ┌─────┴──────┬─────────┐
      │            │          │
      v            v          v
   HEALTHY     DEGRADED     DOWN
      │            │          │
      v            │          v
   Save          Email     AUTO-REPAIR
   Stable        Alert     1. Analyse erreur
   Commit                  2. Vercel deploy hook
                           3. Render restart/rollback
                           4. Email notification
```

## Endpoints Surveilles

| Endpoint | Critique | Description |
|----------|----------|-------------|
| `/api/health` | OUI | API + Supabase |
| `/api/evaluations` | OUI | Evaluations reprise |
| `/api/cron/status` | NON | Etat dernier cron |
| `/api/services/status` | NON | Tous les services |
| `/api/wholesale-contacts` | NON | Contacts grossistes |

## Actions Automatiques

| Status | Action |
|--------|--------|
| **HEALTHY** | Enregistre le commit stable |
| **DEGRADED** | Envoie email alerte |
| **DOWN** | Analyse erreur → Trigger Vercel deploy hook → Restart ou rollback Render → Email |

## Configuration

Tout est dans le workflow `.github/workflows/auto-repair.yml`:

| Variable | Valeur |
|----------|--------|
| `SERVICE_URL` | `https://kenbot-dashboard-api.onrender.com` |
| `SMTP_HOST` | `smtp.gmail.com` |
| `SMTP_PORT` | `587` |
| `SMTP_USER` | `info@luxuradistribution.com` |
| `SMTP_PASS` | App password Gmail |
| `DEPLOY_HOOKS` | URL Vercel Deploy Hook |

## Utilisation Locale

```bash
# Verification manuelle
python monitoring/auto_repair.py

# Mode surveillance continue (Ctrl+C pour arreter)
python monitoring/auto_repair.py --watch

# Forcer une reparation
python monitoring/auto_repair.py --repair

# Rollback au dernier commit stable
python monitoring/auto_repair.py --rollback

# Voir le status
python monitoring/auto_repair.py --status
```

## Declenchement Manuel (GitHub)

1. Allez dans **Actions** > **Kenbot Auto-Repair Monitor**
2. Cliquez **Run workflow**
3. Choisissez: `check`, `repair`, `rollback`, ou `status`

---

*Kennebec Auto — Systeme de monitoring automatique*
