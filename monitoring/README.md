# Kenbot Auto-Repair System

## Vue d'ensemble

Ce systeme surveille automatiquement l'API Kenbot Dashboard et la repare si elle tombe en panne.

## Fonctionnalites

- **Health Check** toutes les 5 minutes via GitHub Actions
- **Auto-repair** si service down (max 5 tentatives)
- **Rollback automatique** vers le dernier commit stable
- **Notifications email** en cas de probleme
- **Detection** des patterns d'erreurs connus

## Configuration Requise

### 1. Secrets GitHub a configurer

Allez dans: **Repository > Settings > Secrets and variables > Actions**

Ajoutez ces secrets:

| Secret | Description | Valeur |
|--------|-------------|--------|
| `SMTP_USER` | Email d'envoi | `info@luxuradistribution.com` |
| `SMTP_PASS` | App password Gmail | `(votre app password)` |

### 2. Activer GitHub Actions

Le workflow s'execute automatiquement toutes les 5 minutes. Vous pouvez aussi le declencher manuellement:

1. Allez dans **Actions** > **Kenbot Auto-Repair Monitor**
2. Cliquez **Run workflow**
3. Choisissez l'action: `check`, `repair`, `rollback`, ou `status`

## Comment ca marche

```
TOUTES LES 5 MINUTES
         |
         v
  +---------------+
  |  Health Check  |
  |  /api/health   |
  |  /api/evals    |
  +-------+-------+
          |
    +-----+------+--------+
    |            |         |
    v            v         v
 HEALTHY    DEGRADED     DOWN
    |            |         |
    v            |         v
 Save Stable    |    AUTO-REPAIR
 Commit         |    1. Analyze
                |    2. Rollback
                |    3. Notify
                |         |
                v         v
         Email si probleme
```

## Scripts locaux

```bash
# Verification manuelle
python monitoring/auto_repair.py

# Mode surveillance continue
python monitoring/auto_repair.py --watch

# Forcer une reparation
python monitoring/auto_repair.py --repair

# Rollback manuel
python monitoring/auto_repair.py --rollback
```

## Endpoints surveilles

| Endpoint | Critique | Description |
|----------|----------|-------------|
| `/api/health` | Oui | Sante globale du service |
| `/api/evaluations` | Oui | Liste des evaluations |
| `/api/wholesale-contacts` | Non | Contacts wholesale |

## Patterns d'erreurs detectes

| Pattern | Action |
|---------|--------|
| `ModuleNotFoundError` | Rollback vers stable |
| `No open ports` | Rollback vers stable |
| `connection.*timeout` | Restart service |

---
*Kennebec Auto - Systeme de monitoring automatique*
