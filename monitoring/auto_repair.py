#!/usr/bin/env python3
"""
KENBOT AUTO-REPAIR SYSTEM V1.0
=================================
Systeme de surveillance et reparation automatique pour Render.
Adapte du template Luxura pour kenbot-dashboard-api.

FONCTIONNALITES:
- Monitoring continu des endpoints
- Detection automatique des erreurs
- Auto-repair avec multiple strategies
- Rollback automatique si deploiement echoue
- Notifications email via SMTP
- Boucle jusqu'a stabilite

USAGE:
  python auto_repair.py           # Mode normal
  python auto_repair.py --watch   # Mode surveillance continue
  python auto_repair.py --repair  # Force une reparation
  python auto_repair.py --rollback # Rollback au dernier commit stable

CRON (toutes les 5 minutes via GitHub Actions):
  */5 * * * *

Environment Variables:
  SMTP_HOST: Serveur SMTP (smtp.gmail.com)
  SMTP_PORT: Port SMTP (587)
  SMTP_USER: Email expediteur
  SMTP_PASS: App password Gmail
  GITHUB_TOKEN: Token GitHub pour les operations git
  RENDER_API_KEY: (optionnel) Pour redemarrer le service via API
"""

import os
import sys
import json
import time
import subprocess
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# =====================================================
# CONFIGURATION
# =====================================================

# URL de base du service Render
RENDER_BASE_URL = os.getenv("RENDER_SERVICE_URL", "https://kenbot-dashboard-api.onrender.com")

# Timeout pour les requetes HTTP
REQUEST_TIMEOUT = 30

# Nombre max de tentatives de reparation
MAX_REPAIR_ATTEMPTS = 5

# Delai entre les tentatives (secondes)
REPAIR_DELAY = 60

# Fichier pour stocker l'etat
STATE_FILE = Path(__file__).parent / ".repair_state.json"

# Fichier pour stocker le dernier commit stable
STABLE_COMMIT_FILE = Path(__file__).parent / ".last_stable_commit"

# Vercel Deploy Hook (declenche un rebuild frontend automatiquement)
VERCEL_DEPLOY_HOOK = os.getenv("DEPLOY_HOOKS", "")

# Email configuration (utilise les variables SMTP du Render)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "info@luxuradistribution.com")
SMTP_PASS = os.getenv("SMTP_PASS", "zgvsfiajermjqpgh")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "info@luxuradistribution.com")

# GitHub configuration
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = "luxurahair/kenbot-dashboard"

# Endpoints critiques a surveiller
CRITICAL_ENDPOINTS = [
    {"path": "/api/health", "method": "GET", "expected_status": 200, "name": "Health", "critical": True},
    {"path": "/api/evaluations", "method": "GET", "expected_status": 200, "name": "Evaluations", "critical": True},
    {"path": "/api/cron/status", "method": "GET", "expected_status": 200, "name": "Cron Status", "critical": False},
    {"path": "/api/services/status", "method": "GET", "expected_status": 200, "name": "Services", "critical": False},
    {"path": "/api/wholesale-contacts", "method": "GET", "expected_status": 200, "name": "Wholesale", "critical": False},
]

# Patterns d'erreurs et leurs fixes automatiques
AUTO_FIX_PATTERNS = {
    "ModuleNotFoundError: No module named": {
        "description": "Module Python manquant",
        "fix_type": "requirements",
        "fix_action": "add_missing_module",
        "severity": "CRITICAL"
    },
    "No open ports detected": {
        "description": "Le serveur ne demarre pas",
        "fix_type": "rollback",
        "fix_action": "rollback_to_stable",
        "severity": "CRITICAL"
    },
    "connection.*timeout": {
        "description": "Timeout base de donnees",
        "fix_type": "restart",
        "fix_action": "restart_service",
        "severity": "HIGH"
    },
    "SUPABASE_URL": {
        "description": "Variable Supabase manquante",
        "fix_type": "env_check",
        "fix_action": "check_env_vars",
        "severity": "HIGH"
    },
    "OPENAI_API_KEY": {
        "description": "Cle API manquante",
        "fix_type": "env_check",
        "fix_action": "check_env_vars",
        "severity": "HIGH"
    }
}

# =====================================================
# LOGGING
# =====================================================

def log(message: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    icons = {
        "INFO": "[INFO]",
        "OK": "[OK]",
        "WARN": "[WARN]",
        "ERROR": "[ERROR]",
        "FIX": "[FIX]",
        "REPAIR": "[REPAIR]",
        "ROLLBACK": "[ROLLBACK]",
        "EMAIL": "[EMAIL]"
    }
    icon = icons.get(level, "[LOG]")
    print(f"[{timestamp}] {icon} {message}")

# =====================================================
# STATE MANAGEMENT
# =====================================================

def load_state() -> Dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "last_check": None,
        "consecutive_failures": 0,
        "repair_attempts": 0,
        "last_repair": None,
        "status": "UNKNOWN"
    }

def save_state(state: Dict):
    state["last_check"] = datetime.now().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2))

def get_last_stable_commit() -> Optional[str]:
    if STABLE_COMMIT_FILE.exists():
        return STABLE_COMMIT_FILE.read_text().strip()
    return None

def save_stable_commit(commit_hash: str):
    STABLE_COMMIT_FILE.write_text(commit_hash)
    log(f"Commit stable enregistre: {commit_hash[:8]}", "OK")

# =====================================================
# HEALTH CHECKS
# =====================================================

def check_endpoint(endpoint: Dict) -> Tuple[bool, str, float]:
    url = f"{RENDER_BASE_URL}{endpoint['path']}"
    start_time = time.time()

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response_time = time.time() - start_time

        if response.status_code == endpoint['expected_status']:
            return True, f"OK ({response_time*1000:.0f}ms)", response_time
        else:
            return False, f"Status {response.status_code}", response_time

    except requests.exceptions.Timeout:
        return False, "TIMEOUT", REQUEST_TIMEOUT
    except requests.exceptions.ConnectionError:
        return False, "CONNECTION_REFUSED", 0
    except Exception as e:
        return False, f"ERROR: {str(e)[:50]}", 0

def run_health_checks() -> Dict:
    results = {
        "timestamp": datetime.now().isoformat(),
        "endpoints": [],
        "passed": 0,
        "failed": 0,
        "critical_failed": False,
        "overall_status": "UNKNOWN"
    }

    for endpoint in CRITICAL_ENDPOINTS:
        success, message, response_time = check_endpoint(endpoint)

        result = {
            "name": endpoint['name'],
            "path": endpoint['path'],
            "success": success,
            "message": message,
            "critical": endpoint.get('critical', False)
        }
        results["endpoints"].append(result)

        if success:
            results["passed"] += 1
            log(f"{endpoint['name']}: {message}", "OK")
        else:
            results["failed"] += 1
            log(f"{endpoint['name']}: {message}", "ERROR")
            if endpoint.get('critical'):
                results["critical_failed"] = True

    if results["failed"] == 0:
        results["overall_status"] = "HEALTHY"
    elif results["critical_failed"]:
        results["overall_status"] = "DOWN"
    else:
        results["overall_status"] = "DEGRADED"

    return results

# =====================================================
# ERROR ANALYSIS
# =====================================================

def fetch_render_logs() -> Optional[str]:
    try:
        response = requests.get(f"{RENDER_BASE_URL}/api/health", timeout=10)
        if response.status_code >= 500:
            return response.text
    except Exception as e:
        return str(e)
    return None

def analyze_error(error_text: str) -> Optional[Dict]:
    import re
    for pattern, fix_info in AUTO_FIX_PATTERNS.items():
        if re.search(pattern, error_text, re.IGNORECASE):
            return {
                "pattern": pattern,
                **fix_info
            }
    return None

# =====================================================
# AUTO-REPAIR ACTIONS
# =====================================================

def git_command(cmd: List[str], cwd: str = None) -> Tuple[bool, str]:
    try:
        result = subprocess.run(
            ["git"] + cmd,
            cwd=cwd or str(Path(__file__).parent.parent),
            capture_output=True,
            text=True,
            timeout=60
        )
        return result.returncode == 0, result.stdout + result.stderr
    except Exception as e:
        return False, str(e)

def get_current_commit() -> Optional[str]:
    success, output = git_command(["rev-parse", "HEAD"])
    if success:
        return output.strip()
    return None

def rollback_to_commit(commit_hash: str) -> bool:
    log(f"Rollback vers {commit_hash[:8]}...", "ROLLBACK")

    success, output = git_command(["reset", "--hard", commit_hash])
    if not success:
        log(f"Echec reset: {output}", "ERROR")
        return False

    success, output = git_command(["push", "--force", "origin", "main"])
    if not success:
        log(f"Echec push: {output}", "ERROR")
        return False

    log(f"Rollback reussi vers {commit_hash[:8]}", "OK")
    return True

def rollback_to_stable() -> bool:
    stable_commit = get_last_stable_commit()
    if not stable_commit:
        log("Aucun commit stable enregistre", "ERROR")
        return False

    current_commit = get_current_commit()
    if current_commit == stable_commit:
        log("Deja sur le commit stable", "INFO")
        return True

    return rollback_to_commit(stable_commit)

def restart_service() -> bool:
    log("Tentative de redemarrage du service...", "REPAIR")

    # 1. Trigger Vercel redeploy via hook
    try:
        if VERCEL_DEPLOY_HOOK:
            r = requests.post(VERCEL_DEPLOY_HOOK, timeout=15)
            if r.status_code == 200:
                log("Vercel redeploy declenche via hook", "OK")
    except Exception as e:
        log(f"Vercel hook error: {e}", "WARN")

    # 2. Trigger Render redeploy via empty commit
    render_api_key = os.getenv("RENDER_API_KEY")
    if render_api_key:
        try:
            pass
        except Exception:
            pass

    success, _ = git_command(["commit", "--allow-empty", "-m", "Auto-repair: trigger redeploy"])
    if success:
        success, _ = git_command(["push", "origin", "main"])
        if success:
            log("Redeploiement declenche", "OK")
            return True

    log("Impossible de redemarrer le service", "ERROR")
    return False

# =====================================================
# EMAIL NOTIFICATIONS
# =====================================================

def send_notification(subject: str, body: str, is_html: bool = False):
    if not SMTP_PASS:
        log("Email non configure (SMTP_PASS manquant)", "WARN")
        return False

    try:
        msg = MIMEMultipart()
        msg['Subject'] = f"Kenbot Monitor: {subject}"
        msg['From'] = SMTP_USER
        msg['To'] = ADMIN_EMAIL

        content_type = 'html' if is_html else 'plain'
        msg.attach(MIMEText(body, content_type))

        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)

        log(f"Email envoye: {subject}", "EMAIL")
        return True

    except Exception as e:
        log(f"Erreur envoi email: {e}", "ERROR")
        return False

def send_status_report(health_results: Dict, repair_actions: List[str] = None):
    status = health_results["overall_status"]

    html = f"""
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
        .status-healthy {{ color: #28a745; }}
        .status-degraded {{ color: #ffc107; }}
        .status-down {{ color: #dc3545; }}
        .endpoint {{ padding: 5px 10px; margin: 5px 0; border-radius: 4px; }}
        .endpoint-ok {{ background: #d4edda; }}
        .endpoint-fail {{ background: #f8d7da; }}
        .actions {{ background: #fff3cd; padding: 15px; border-radius: 8px; margin-top: 20px; }}
    </style>
    </head>
    <body>
        <h1>Kenbot Dashboard API Monitor</h1>
        <h2 class="status-{status.lower()}">Status: {status}</h2>
        <p>Timestamp: {health_results['timestamp']}</p>
        <h3>Endpoints</h3>
    """

    for ep in health_results["endpoints"]:
        css_class = "endpoint-ok" if ep["success"] else "endpoint-fail"
        icon = "OK" if ep["success"] else "FAIL"
        html += f'<div class="endpoint {css_class}">[{icon}] {ep["name"]}: {ep["message"]}</div>'

    if repair_actions:
        html += '<div class="actions"><h3>Actions de reparation</h3><ul>'
        for action in repair_actions:
            html += f'<li>{action}</li>'
        html += '</ul></div>'

    html += """
        <p style="color: #666; margin-top: 30px;">
            Kennebec Auto - Systeme de monitoring automatique
        </p>
    </body>
    </html>
    """

    subject = f"{status} - Kenbot Dashboard API"
    send_notification(subject, html, is_html=True)

# =====================================================
# MAIN REPAIR LOOP
# =====================================================

def attempt_repair(health_results: Dict, state: Dict) -> bool:
    repair_actions = []

    log("=" * 50, "INFO")
    log("DEMARRAGE AUTO-REPAIR", "REPAIR")
    log("=" * 50, "INFO")

    if state["repair_attempts"] >= MAX_REPAIR_ATTEMPTS:
        log(f"Max tentatives atteint ({MAX_REPAIR_ATTEMPTS})", "ERROR")
        repair_actions.append(f"Maximum de {MAX_REPAIR_ATTEMPTS} tentatives atteint")

        if rollback_to_stable():
            repair_actions.append("Rollback vers commit stable effectue")
        else:
            repair_actions.append("Rollback echoue - intervention manuelle requise")

        send_status_report(health_results, repair_actions)
        return False

    state["repair_attempts"] += 1
    state["last_repair"] = datetime.now().isoformat()

    error_text = fetch_render_logs()
    fix_info = None
    if error_text:
        fix_info = analyze_error(error_text)

    repair_success = False

    if fix_info:
        log(f"Erreur detectee: {fix_info['description']}", "INFO")
        repair_actions.append(f"Erreur: {fix_info['description']}")

        if fix_info['fix_action'] == 'rollback_to_stable':
            repair_success = rollback_to_stable()
            repair_actions.append("Rollback vers commit stable")

        elif fix_info['fix_action'] == 'restart_service':
            repair_success = restart_service()
            repair_actions.append("Redemarrage du service")
        else:
            repair_success = restart_service()
            repair_actions.append("Redemarrage du service (fix generique)")
    else:
        log("Aucune erreur specifique detectee, tentative de restart", "INFO")
        repair_success = restart_service()
        repair_actions.append("Redemarrage du service (diagnostic general)")

    if repair_success:
        log(f"Reparation tentative #{state['repair_attempts']} lancee", "OK")
        repair_actions.append(f"Tentative #{state['repair_attempts']} en cours")
    else:
        log(f"Reparation tentative #{state['repair_attempts']} echouee", "ERROR")
        repair_actions.append(f"Tentative #{state['repair_attempts']} echouee")

    send_status_report(health_results, repair_actions)
    save_state(state)
    return repair_success

def monitor_and_repair():
    state = load_state()

    log("=" * 60, "INFO")
    log("KENBOT AUTO-REPAIR SYSTEM", "INFO")
    log("=" * 60, "INFO")

    health_results = run_health_checks()

    log("-" * 60, "INFO")
    log(f"Status global: {health_results['overall_status']}",
        "OK" if health_results['overall_status'] == "HEALTHY" else "ERROR")

    if health_results['overall_status'] == "HEALTHY":
        current_commit = get_current_commit()
        if current_commit:
            save_stable_commit(current_commit)

        state["consecutive_failures"] = 0
        state["repair_attempts"] = 0
        state["status"] = "HEALTHY"
        save_state(state)

        log("Service stable - aucune action requise", "OK")
        return True

    state["consecutive_failures"] += 1
    state["status"] = health_results['overall_status']

    log(f"Echecs consecutifs: {state['consecutive_failures']}", "WARN")

    if state["consecutive_failures"] == 1:
        log("Premier echec detecte, surveillance...", "WARN")
        save_state(state)
        return False

    return attempt_repair(health_results, state)

def watch_mode():
    log("Mode surveillance active (Ctrl+C pour arreter)", "INFO")

    while True:
        try:
            result = monitor_and_repair()

            if result:
                log("Prochaine verification dans 5 minutes...", "INFO")
                time.sleep(300)
            else:
                log(f"Prochaine verification dans {REPAIR_DELAY} secondes...", "INFO")
                time.sleep(REPAIR_DELAY)

        except KeyboardInterrupt:
            log("Surveillance arretee par l'utilisateur", "INFO")
            break
        except Exception as e:
            log(f"Erreur dans la boucle: {e}", "ERROR")
            time.sleep(60)

# =====================================================
# CLI
# =====================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Kenbot Auto-Repair System")
    parser.add_argument("--watch", action="store_true", help="Mode surveillance continue")
    parser.add_argument("--repair", action="store_true", help="Force une reparation")
    parser.add_argument("--rollback", action="store_true", help="Rollback au dernier commit stable")
    parser.add_argument("--status", action="store_true", help="Affiche le status actuel")

    args = parser.parse_args()

    if args.watch:
        watch_mode()
    elif args.repair:
        state = load_state()
        health_results = run_health_checks()
        attempt_repair(health_results, state)
    elif args.rollback:
        rollback_to_stable()
    elif args.status:
        state = load_state()
        print(json.dumps(state, indent=2))
    else:
        result = monitor_and_repair()
        sys.exit(0 if result else 1)

if __name__ == "__main__":
    main()
