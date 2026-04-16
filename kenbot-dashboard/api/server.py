from fastapi import FastAPI, APIRouter, UploadFile, File, Form as FastForm
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
import os
import logging
from pathlib import Path
from pydantic import BaseModel, ConfigDict
from typing import Optional, Dict, Any, List
import uuid
import requests as http_req
from datetime import datetime, timezone
from supabase import create_client as sb_create_client

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env', override=False)

# Supabase (for kenbot real data)
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
sb = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        sb = sb_create_client(SUPABASE_URL, SUPABASE_KEY)
        print(f"[STARTUP] Supabase connected to {SUPABASE_URL[:40]}...")
    except Exception as e:
        print(f"[STARTUP] Supabase connection failed: {e}")
else:
    print("[STARTUP] Supabase credentials not set - running without data")

app = FastAPI(title="Kenbot Dashboard API")

# CORS - must be before router
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

api_router = APIRouter(prefix="/api")

# ─── Supabase helpers ───
def sb_query(table, select="*", filters=None, order=None, limit=None, count=False):
    if not sb:
        return {"data": [], "count": 0}
    try:
        q = sb.table(table).select(select, count="exact" if count else "planned")
        if filters:
            for key, val in filters.items():
                q = q.eq(key, val)
        if order:
            q = q.order(order, desc=True)
        if limit:
            q = q.limit(limit)
        result = q.execute()
        return {"data": result.data or [], "count": result.count if count else len(result.data or [])}
    except Exception as e:
        logging.error(f"Supabase query error on {table}: {e}")
        return {"data": [], "count": 0}

# ─── Changelog ───
CHANGELOG = [
    {
        "version": "4.0.0",
        "date": "2026-04-12",
        "type": "feature",
        "title": "Refonte detection par STOCK + UNSOLD + SEO + Cleanup",
        "changes": [
            {"severity": "critical", "description": "Toutes les comparaisons (SOLD, PRICE_CHANGED, PHOTOS_ADDED) utilisent maintenant le STOCK comme cle primaire au lieu du slug", "file": "runner_cron_prod.py"},
            {"severity": "critical", "description": "Nouveau event UNSOLD: restaure automatiquement les posts marques VENDU par erreur si le stock est encore sur Kennebec", "file": "runner_cron_prod.py"},
            {"severity": "critical", "description": "Detection NO_PHOTO par comparaison FB vs Kennebec (fb_photos <= 1 ET kennebec > 1)", "file": "runner_cron_prod.py"},
            {"severity": "critical", "description": "FIX publish_with_photos inexistant — remplace par publish_photos_unpublished + create_post_with_attached_media", "file": "runner_cron_prod.py"},
            {"severity": "critical", "description": "FIX double footer — ad_builder.py ne rajoute plus les echanges, footer_utils.py est l'unique source", "file": "ad_builder.py"},
            {"severity": "medium", "description": "Cleanup automatique: a chaque cron, corrige les posts FB existants avec double footer (max 10/run)", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "Hashtags SEO dynamiques par vehicule (#DodgeHornet2024 #Beauce #SaintGeorges #Pickup etc.)", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "Intro PRICE_CHANGED amelioree: affiche le montant du rabais (ex: 2 000 $ DE RABAIS)", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "Prix fallback depuis inventory DB si le scrape ne trouve pas le prix", "file": "runner_cron_prod.py"},
            {"severity": "low", "description": "Protection anti-boucle SOLD: cooldown 3 jours, verification par stock", "file": "runner_cron_prod.py"},
        ]
    },
    {
        "version": "3.5.0",
        "date": "2026-04-12",
        "type": "feature",
        "title": "Pre-cache PDFs Stellantis 2018+ + VIN strict 17 chars",
        "changes": [
            {"severity": "critical", "description": "Pre-cache obligatoire: au debut du cron, telecharge/verifie les PDFs sticker pour TOUS les Stellantis 2018+", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "ensure_sticker_cached retourne pdf_bytes directement (plus de double telechargement)", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "upsert_sticker_pdf isole dans try/except (FK ne casse plus le return)", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "upsert_scrape_run() execute AVANT le pre-cache (corrige FK sticker_pdfs_run_id_fkey)", "file": "runner_cron_prod.py"},
            {"severity": "low", "description": "VIN decode strictement 17 caracteres (plus de faux positifs)", "file": "vin_decoder.py"},
        ]
    },
    {
        "version": "3.2.0",
        "date": "2026-04-12",
        "type": "bugfix",
        "title": "FIX upsert_post duplicate key + suppression photos commentaires",
        "changes": [
            {"severity": "critical", "description": "upsert_post: on_conflict passe de 'stock' a 'slug' (PK) avec fallback update 3 niveaux", "file": "supabase_db.py"},
            {"severity": "critical", "description": "publish_photos_as_comment_batch supprime (causait 403 FB). Max 10 photos par post, pas de commentaires.", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "Double appel _build_ad_text dans PHOTOS_ADDED corrige: reutilise msg deja genere", "file": "runner_cron_prod.py"},
        ]
    },
    {
        "version": "3.0.0",
        "date": "2026-04-11",
        "type": "feature",
        "title": "AI v3.0 — Generation intelligente par vehicule",
        "changes": [
            {"severity": "critical", "description": "Nouveau moteur de texte llm_v3.py: prompts adaptes par type de vehicule (muscle_car, off_road, suv_premium, pickup, etc.)", "file": "llm_v3.py"},
            {"severity": "critical", "description": "Module vehicle_intelligence.py: parsing titre, detection marque/modele/trim, specs moteur (HP, engine), base 27 marques, 43 modeles, 194 trims", "file": "vehicle_intelligence.py"},
            {"severity": "critical", "description": "Decodage VIN via NHTSA API (vin_decoder.py): moteur, HP, transmission, 4WD, places, securite", "file": "vin_decoder.py"},
            {"severity": "medium", "description": "Humanisation sticker Stellantis: intro AI + options ✅ MAJUSCULES / ▫️ minuscules preservees", "file": "runner_cron_prod.py"},
            {"severity": "medium", "description": "Detection et marquage SOLD sur Facebook (🚨 VENDU 🚨)", "file": "runner_cron_prod.py"},
        ]
    },
]

# ─── Architecture ───
ARCHITECTURE = {
    "components": [
        {"id": "website", "name": "Site Kennebec", "type": "external", "description": "kennebecdodge.ca — Source inventaire (3 pages scrappees)"},
        {"id": "scraper", "name": "kennebec_scrape.py", "type": "module", "description": "Scraping HTML + extraction VIN + photos + prix"},
        {"id": "runner", "name": "runner_cron_prod.py", "type": "core", "description": "Orchestrateur cron — Detection par STOCK: NEW / SOLD / UNSOLD / PRICE_CHANGED / PHOTOS_ADDED / CLEANUP"},
        {"id": "supabase", "name": "Supabase PostgreSQL", "type": "storage", "description": "Tables: inventory, posts, events, scrape_runs, sticker_pdfs + Storage (PDFs, photos)"},
        {"id": "vin_decoder", "name": "vin_decoder.py", "type": "module", "description": "Decodage VIN 17 chars via NHTSA API — moteur, HP, transmission, 4WD, places"},
        {"id": "vehicle_intel", "name": "vehicle_intelligence.py", "type": "module", "description": "Base de connaissance: 27 marques, 43 modeles, 194 trims avec specs, vibe, ton marketing"},
        {"id": "sticker", "name": "sticker_to_ad.py + ad_builder.py", "type": "module", "description": "Extraction PDF Window Sticker (Stellantis 2018+) → options ✅/▫️ structurees"},
        {"id": "llm_v3", "name": "llm_v3.py", "type": "module", "description": "Generation IA GPT-4o — prompts adaptes par type vehicule, 5 styles d'intro, anti-cliches"},
        {"id": "footer", "name": "footer_utils.py", "type": "module", "description": "Footer unique Daniel Giroux — echanges, telephone, hashtags SEO dynamiques"},
        {"id": "facebook", "name": "fb_api.py", "type": "external", "description": "Facebook Graph API — publish, update, delete, fetch feed (max 10 photos/post)"},
        {"id": "meta_feed", "name": "meta_compare_supabase.py", "type": "output", "description": "Rapport CSV: comparaison Meta FB vs site Kennebec"},
        {"id": "dashboard", "name": "kenbot-dashboard", "type": "webapp", "description": "Dashboard React + FastAPI — Cockpit, Preview, Inventaire (Vercel + Render)"},
    ],
    "flows": [
        {"from": "website", "to": "scraper", "label": "HTML 3 pages"},
        {"from": "scraper", "to": "runner", "label": "47 vehicules + VIN + photos"},
        {"from": "runner", "to": "supabase", "label": "Upsert inventory, posts, events"},
        {"from": "runner", "to": "vin_decoder", "label": "VIN → NHTSA specs"},
        {"from": "runner", "to": "vehicle_intel", "label": "Titre → marque/modele/trim"},
        {"from": "runner", "to": "sticker", "label": "PDF Stellantis 2018+ → options"},
        {"from": "runner", "to": "llm_v3", "label": "Specs + options → texte IA"},
        {"from": "runner", "to": "footer", "label": "Texte → footer + hashtags SEO"},
        {"from": "runner", "to": "facebook", "label": "Publish / Update / Delete"},
        {"from": "runner", "to": "meta_feed", "label": "CSV meta_vs_site.csv"},
        {"from": "supabase", "to": "dashboard", "label": "Live data"},
    ],
    "states": ["NEW", "SOLD", "UNSOLD", "PRICE_CHANGED", "PHOTOS_ADDED", "CLEANUP"],
    "pipeline": {
        "order": "UNSOLD → PHOTOS_ADDED → PRICE_CHANGED → NEW → SOLD → CLEANUP",
        "comparison_key": "STOCK (pas slug)",
        "sold_protection": "3 jours cooldown + verification stock Kennebec",
        "sticker_precache": "38 Stellantis 2018+ au debut de chaque run",
    }
}

# ─── Routes ───

@api_router.get("/")
async def root():
    return {"message": "Kenbot Dashboard API", "version": "4.0.0", "supabase_connected": sb is not None}

@api_router.get("/system/status")
async def get_system_status():
    if sb:
        inv_total = sb_query("inventory", "status", count=True)
        inv_active = sb_query("inventory", "status", filters={"status": "ACTIVE"}, count=True)
        inv_sold = sb_query("inventory", "status", filters={"status": "SOLD"}, count=True)
        posts_total = sb_query("posts", "status", count=True)
        posts_active = sb_query("posts", "status", filters={"status": "ACTIVE"}, count=True)
        events_total = sb_query("events", "id", count=True)
        
        # Check no_photo posts via base_text hints
        all_active_posts = sb_query("posts", "slug,base_text,stock", filters={"status": "ACTIVE"}, limit=500)
        no_photo_count = 0
        for p in all_active_posts["data"]:
            bt = (p.get("base_text") or "").lower()
            has_no_photo = p.get("no_photo")
            if has_no_photo is True or "photos suivront" in bt or "photo non disponible" in bt or "sans photo" in bt or "photo a venir" in bt:
                no_photo_count += 1

        # Last events
        last_events = sb_query("events", "*", order="created_at", limit=1)
        last_event = last_events["data"][0] if last_events["data"] else None

        return {
            "version": "2.1.0",
            "supabase_connected": True,
            "stats": {
                "inventory": {"total": inv_total["count"], "active": inv_active["count"], "sold": inv_sold["count"]},
                "posts": {"total": posts_total["count"], "active": posts_active["count"], "no_photo": no_photo_count, "with_photos": posts_active["count"] - no_photo_count},
                "events": {"total": events_total["count"]},
            },
            "last_event": {
                "slug": last_event.get("slug", ""),
                "type": last_event.get("type", ""),
                "timestamp": last_event.get("created_at", ""),
            } if last_event else None,
        }
    
    return {"version": "2.1.0", "supabase_connected": False, "stats": {}}


@api_router.get("/cron/status")
async def get_cron_status():
    """Retourne l'etat du dernier cron sync (scrape_runs + derniers events)."""
    if not sb:
        return {"status": "disconnected", "message": "Supabase non connectee"}
    try:
        # Dernier scrape run
        last_run = sb.table("scrape_runs").select("*").order("created_at", desc=True).limit(1).execute()
        run_data = last_run.data[0] if last_run.data else None

        # 5 derniers events
        last_events = sb.table("events").select("slug,type,created_at").order("created_at", desc=True).limit(5).execute()

        # Stats rapides
        total_runs = sb.table("scrape_runs").select("run_id", count="exact").execute()

        cron_status = "ok"
        message = "Cron operationnel"
        if run_data:
            if run_data.get("status", "").upper() not in ("OK", "SUCCESS", "DONE"):
                cron_status = "warning"
                message = f"Dernier run: {run_data.get('status', 'inconnu')}"

        return {
            "status": cron_status,
            "message": message,
            "last_run": {
                "run_id": run_data.get("run_id", "") if run_data else None,
                "created_at": run_data.get("created_at", "") if run_data else None,
                "status": run_data.get("status", "") if run_data else None,
                "note": run_data.get("note", "") if run_data else None,
            },
            "total_runs": total_runs.count if hasattr(total_runs, 'count') else len(total_runs.data or []),
            "recent_events": [{"slug": e.get("slug",""), "type": e.get("type",""), "at": e.get("created_at","")} for e in (last_events.data or [])],
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


@api_router.get("/services/status")
async def get_services_status():
    """Retourne l'etat de tous les services Kenbot."""
    import requests as http_check
    services = {}

    # 1. Supabase DB
    services["supabase"] = {"status": "connected" if sb else "disconnected", "url": SUPABASE_URL[:40] + "..."}

    # 2. API locale (self)
    services["api"] = {"status": "ok", "service": "kenbot-dashboard-api"}

    # 3. Cron (dernier run)
    if sb:
        try:
            lr = sb.table("scrape_runs").select("run_id,status,created_at").order("created_at", desc=True).limit(1).execute()
            if lr.data:
                run = lr.data[0]
                age_ok = True
                try:
                    from datetime import datetime, timezone, timedelta
                    run_time = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
                    age_ok = (datetime.now(timezone.utc) - run_time) < timedelta(hours=6)
                except Exception:
                    pass
                services["cron"] = {
                    "status": "ok" if run.get("status","").upper() in ("OK","SUCCESS","DONE") and age_ok else "warning",
                    "last_run": run.get("created_at",""),
                    "run_status": run.get("status",""),
                }
            else:
                services["cron"] = {"status": "no_data", "message": "Aucun run trouve"}
        except Exception as e:
            services["cron"] = {"status": "error", "message": str(e)}
    else:
        services["cron"] = {"status": "disconnected"}

    # 4. SMTP
    smtp_ok = bool(SMTP_USER and SMTP_PASS)
    services["smtp"] = {"status": "configured" if smtp_ok else "not_configured", "user": SMTP_USER[:20] + "..." if SMTP_USER else ""}

    # 5. Vercel frontend
    try:
        vr = http_check.get("https://kenbot-dashboard-five.vercel.app", timeout=10)
        services["vercel"] = {"status": "ok" if vr.status_code == 200 else "down", "http": vr.status_code}
    except Exception:
        services["vercel"] = {"status": "unreachable"}

    # Status global
    all_ok = all(s.get("status") in ("ok", "connected", "configured") for s in services.values())
    return {
        "overall": "healthy" if all_ok else "degraded",
        "services": services,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@api_router.get("/inventory")
async def get_inventory(status: Optional[str] = None, limit: int = 200):
    filters = {}
    if status:
        filters["status"] = status.upper()
    result = sb_query("inventory", "*", filters=filters, order="updated_at", limit=limit)
    return result["data"]

@api_router.get("/inventory/stats")
async def get_inventory_stats():
    total = sb_query("inventory", "status", count=True)
    active = sb_query("inventory", "status", filters={"status": "ACTIVE"}, count=True)
    sold = sb_query("inventory", "status", filters={"status": "SOLD"}, count=True)
    return {"total": total["count"], "active": active["count"], "sold": sold["count"]}

@api_router.get("/posts")
async def get_posts(status: Optional[str] = None, limit: int = 200):
    filters = {}
    if status:
        filters["status"] = status.upper()
    result = sb_query("posts", "*", filters=filters, order="published_at", limit=limit)
    
    # Detect no_photo from base_text hints if column doesn't exist
    for p in result["data"]:
        if "no_photo" not in p or p.get("no_photo") is None:
            bt = (p.get("base_text") or "").lower()
            p["no_photo"] = (
                "photos suivront" in bt or
                "photo non disponible" in bt or
                "sans photo" in bt or
                "photo a venir" in bt or
                "photos a venir" in bt or
                "nouveau vehicule en inventaire" in bt
            )
        if "photo_count" not in p or p.get("photo_count") is None:
            p["photo_count"] = 0 if p.get("no_photo") else -1
    
    return result["data"]

@api_router.get("/posts/stats")
async def get_posts_stats():
    total = sb_query("posts", "status", count=True)
    active = sb_query("posts", "status", filters={"status": "ACTIVE"}, count=True)
    sold = sb_query("posts", "status", filters={"status": "SOLD"}, count=True)
    
    all_active = sb_query("posts", "slug,base_text,stock", filters={"status": "ACTIVE"}, limit=500)
    no_photo = 0
    for p in all_active["data"]:
        bt = (p.get("base_text") or "").lower()
        has_flag = p.get("no_photo")
        if has_flag is True or "photos suivront" in bt or "photo non disponible" in bt or "sans photo" in bt:
            no_photo += 1
    
    return {"total": total["count"], "active": active["count"], "sold": sold["count"], "no_photo": no_photo, "with_photos": active["count"] - no_photo}

@api_router.get("/events")
async def get_events(limit: int = 50):
    result = sb_query("events", "*", order="created_at", limit=limit)
    return result["data"]

@api_router.get("/events/recent")
async def get_recent_events(limit: int = 20):
    result = sb_query("events", "*", order="created_at", limit=limit)
    # Group by type
    type_counts = {}
    for e in result["data"]:
        t = e.get("type", "UNKNOWN")
        type_counts[t] = type_counts.get(t, 0) + 1
    return {"events": result["data"], "type_counts": type_counts}

@api_router.get("/scrape-runs")
async def get_scrape_runs(limit: int = 20):
    result = sb_query("scrape_runs", "*", order="created_at", limit=limit)
    return result["data"]

@api_router.get("/sticker-pdfs")
async def get_sticker_pdfs(limit: int = 50):
    result = sb_query("sticker_pdfs", "*", order="created_at", limit=limit)
    return result["data"]

class RunOptions(BaseModel):
    model_config = ConfigDict(extra="ignore")
    dry_run: bool = False
    max_targets: int = 4
    force_stock: Optional[str] = None

@api_router.post("/trigger/run")
async def trigger_run(options: RunOptions = RunOptions()):
    if not sb:
        return {"ok": False, "message": "Supabase non connecte"}
    try:
        payload = {
            "dry_run": options.dry_run,
            "max_targets": options.max_targets,
            "force_stock": options.force_stock,
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": "kenbot-dashboard",
        }
        sb.table("events").insert({
            "slug": "BOOT",
            "type": "RUN_REQUESTED",
            "payload": payload,
        }).execute()
        return {"ok": True, "message": "Run demande! Le prochain cron va l'executer.", "payload": payload}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@api_router.post("/trigger/force-stock")
async def trigger_force_stock(stock: str):
    if not sb:
        return {"ok": False, "message": "Supabase non connecte"}
    try:
        payload = {
            "force_stock": stock.strip().upper(),
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": "kenbot-dashboard",
        }
        sb.table("events").insert({
            "slug": "BOOT",
            "type": "FORCE_STOCK_REQUESTED",
            "payload": payload,
        }).execute()
        return {"ok": True, "message": f"Force stock {stock} demande!", "payload": payload}
    except Exception as e:
        return {"ok": False, "message": str(e)}


@api_router.get("/vehicles/compare")
async def get_vehicles_compare():
    """
    Comparaison Kennebec (inventaire) vs Facebook (posts).
    Retourne chaque véhicule avec son statut sur les deux plateformes.
    """
    if not sb:
        return {"vehicles": [], "stats": {}}

    try:
        inv_result = sb.table("inventory").select("slug,stock,title,vin,price_int,km_int,status,updated_at").order("updated_at", desc=True).limit(500).execute()
        posts_result = sb.table("posts").select("slug,stock,post_id,status,base_text,no_photo,photo_count,published_at,last_updated_at,sold_at").limit(500).execute()

        inv_data = inv_result.data or []
        posts_data = posts_result.data or []

        # Index posts par stock
        posts_by_stock = {}
        for p in posts_data:
            st = (p.get("stock") or "").strip().upper()
            if st:
                posts_by_stock[st] = p

        vehicles = []
        for inv in inv_data:
            stock = (inv.get("stock") or "").strip().upper()
            if not stock:
                continue

            post = posts_by_stock.get(stock)
            inv_status = (inv.get("status") or "").upper()

            # Déterminer le statut FB
            fb_status = "AUCUN POST"
            fb_post_id = ""
            fb_photos = 0
            fb_published = ""
            fb_updated = ""
            fb_no_photo = False

            if post:
                fb_status = (post.get("status") or "INCONNU").upper()
                fb_post_id = post.get("post_id") or ""
                fb_photos = post.get("photo_count") or 0
                fb_published = post.get("published_at") or ""
                fb_updated = post.get("last_updated_at") or ""
                fb_no_photo = post.get("no_photo") is True

                # Détecter no_photo depuis le texte
                if not fb_no_photo and fb_photos <= 1:
                    bt = (post.get("base_text") or "").lower()
                    fb_no_photo = any(h in bt for h in ["photos suivront", "sans photo", "photo non disponible"])

            # Problème détecté?
            problem = ""
            if inv_status == "ACTIVE" and fb_status == "SOLD":
                problem = "FAUX VENDU"
            elif inv_status == "ACTIVE" and fb_status == "AUCUN POST":
                problem = "PAS SUR FB"
            elif inv_status == "ACTIVE" and fb_no_photo:
                problem = "SANS PHOTO"
            elif inv_status == "SOLD" and fb_status == "ACTIVE":
                problem = "FB PAS MAJ"

            vehicles.append({
                "stock": stock,
                "title": inv.get("title") or "",
                "price": inv.get("price_int"),
                "km": inv.get("km_int"),
                "vin": inv.get("vin") or "",
                "kennebec_status": inv_status,
                "fb_status": fb_status,
                "fb_post_id": fb_post_id,
                "fb_photos": fb_photos,
                "fb_no_photo": fb_no_photo,
                "fb_published": fb_published,
                "fb_updated": fb_updated,
                "problem": problem,
                "updated_at": inv.get("updated_at") or "",
            })

        # Stats
        total = len(vehicles)
        on_kennebec = sum(1 for v in vehicles if v["kennebec_status"] == "ACTIVE")
        on_fb_active = sum(1 for v in vehicles if v["fb_status"] == "ACTIVE")
        on_fb_sold = sum(1 for v in vehicles if v["fb_status"] == "SOLD")
        no_fb = sum(1 for v in vehicles if v["fb_status"] == "AUCUN POST" and v["kennebec_status"] == "ACTIVE")
        faux_vendu = sum(1 for v in vehicles if v["problem"] == "FAUX VENDU")
        sans_photo = sum(1 for v in vehicles if v["problem"] == "SANS PHOTO")
        problems = sum(1 for v in vehicles if v["problem"])

        return {
            "vehicles": vehicles,
            "stats": {
                "total": total,
                "kennebec_active": on_kennebec,
                "fb_active": on_fb_active,
                "fb_sold": on_fb_sold,
                "no_fb_post": no_fb,
                "faux_vendu": faux_vendu,
                "sans_photo": sans_photo,
                "problems": problems,
            }
        }
    except Exception as e:
        logging.error(f"Compare error: {e}")
        return {"vehicles": [], "stats": {}, "error": str(e)}

@api_router.get("/changelog")
async def get_changelog():
    return CHANGELOG

@api_router.get("/vehicle-intelligence/{stock}")
async def get_vehicle_intelligence(stock: str):
    """Retourne le profil intelligent d'un véhicule par stock."""
    if not sb:
        return {"error": "Supabase non connecte"}
    try:
        from vehicle_intelligence import build_vehicle_context
        result = sb.table("inventory").select("*").eq("stock", stock.upper()).limit(1).execute()
        if not result.data:
            return {"error": f"Vehicule {stock} non trouve"}
        vehicle = result.data[0]
        ctx = build_vehicle_context(vehicle)
        return {"vehicle": vehicle, "intelligence": ctx}
    except Exception as e:
        return {"error": str(e)}

@api_router.post("/generate-text/{stock}")
async def generate_text_for_vehicle(stock: str, event: str = "NEW"):
    """Génère un texte Facebook intelligent pour un véhicule via Emergent LLM."""
    if not sb:
        return {"ok": False, "error": "Supabase non connecte"}
    try:
        from vehicle_intelligence import build_vehicle_context, humanize_options
        from openai import AsyncOpenAI

        result = sb.table("inventory").select("*").eq("stock", stock.upper()).limit(1).execute()
        if not result.data:
            return {"ok": False, "error": f"Vehicule {stock} non trouve"}
        vehicle = result.data[0]
        ctx = build_vehicle_context(vehicle)

        # Decode VIN via NHTSA pour enrichir les specs
        vin_specs = None
        vin_specs_text = ""
        vin_val = (vehicle.get("vin") or "").strip()
        if len(vin_val) >= 11:
            try:
                from vin_decoder import decode_vin, format_specs_for_prompt, format_engine_line
                vin_specs = decode_vin(vin_val)
                if vin_specs:
                    vin_specs_text = format_specs_for_prompt(vin_specs)
                    # Enrichir le ctx avec les specs NHTSA si vehicle_intelligence n'a pas trouve
                    if not ctx.get("hp") and vin_specs.get("engine_hp"):
                        ctx["hp"] = vin_specs["engine_hp"]
                        eng = format_engine_line(vin_specs)
                        # Eviter "340 HP" en double dans l'affichage
                        ctx["engine"] = eng.replace(f" — {vin_specs['engine_hp']} HP", "")
            except Exception as e:
                print(f"[VIN_DECODE] {vin_val}: {e}")

        # Get sticker options if available
        options_text = ""
        post_result = sb.table("posts").select("base_text").eq("stock", stock.upper()).limit(1).execute()
        if post_result.data and post_result.data[0].get("base_text"):
            bt = post_result.data[0]["base_text"]
            if "ACCESSOIRES" in bt or "QUIPEMENTS" in bt:
                options_text = bt

        human_options = humanize_options(options_text) if options_text else []

        # Build specs info (avoid duplication with vin_specs_text)
        specs_info = []
        if not vin_specs_text:
            # Only add from vehicle_intelligence if we don't have NHTSA data
            if ctx.get("hp"):
                specs_info.append(f"Moteur: {ctx['engine']} — {ctx['hp']} chevaux")
            elif ctx.get("engine"):
                specs_info.append(f"Moteur: {ctx['engine']}")
        if ctx.get("trim_vibe"):
            specs_info.append(f"Ce trim: {ctx['trim_vibe']}")
        if ctx.get("model_known_for"):
            specs_info.append(f"Ce modele est connu pour: {ctx['model_known_for']}")
        if ctx.get("brand_identity"):
            specs_info.append(f"La marque {ctx['brand'].capitalize()}: {ctx['brand_identity']}")

        vtype = ctx.get("vehicle_type", "general")
        tone_map = {
            "muscle_car": "adrenaline et son du moteur",
            "muscle_sedan": "puissance et style 4 portes",
            "pickup": "robustesse et capacite",
            "pickup_hd": "robustesse et capacite",
            "off_road": "aventure et liberte",
            "suv_premium": "confort et raffinement",
            "citadine": "style et economie",
            "suv_compact": "style et economie",
            "exotique": "exclusivite et reve",
            "collector": "exclusivite et reve",
            "berline": "confort et fiabilite au quotidien",
            "minivan": "espace familial et polyvalence",
            "commercial": "efficacite et espace de travail",
        }
        tone = tone_map.get(vtype, "polyvalence et fiabilite")

        import random
        styles = ["direct", "storytelling", "question", "expertise", "opportunite"]
        style = random.choice(styles)

        system_msg = """Tu es Daniel Giroux, vendeur passionne chez Kennebec Dodge Chrysler a Saint-Georges en Beauce.
Tu ecris des annonces Facebook pour des vehicules d'occasion.

REGLES ABSOLUES:
- Tu ecris en francais quebecois naturel. Pas de francais de France. Pas de robot.
- Tu parles comme un VRAI vendeur qui connait ses chars. Pas de phrases generiques.
- JAMAIS de "Pret a dominer les routes" ou "faire tourner les tetes" — c'est cliche.
- JAMAIS de "sillonner la Beauce" ou "conquerir les chemins" — c'est du robot.
- JAMAIS mentionner "la Beauce", "routes de la Beauce" ou "paysages beauceron". On vend des chars, pas du tourisme.
- ABSOLUMENT AUCUN mot vulgaire, grossier ou a caractere sexuel. Pas de "couilles", "balls", "badass", "bitch", "cul", "merde" ou tout autre sacre/juron. C'est une page PROFESSIONNELLE d'un concessionnaire. Le ton est passionne mais TOUJOURS respectueux et professionnel.
- Chaque texte doit etre UNIQUE. Si tu vends un Challenger, parle du V8. Si c'est un Wrangler, parle du off-road.
- Le ton est direct, authentique, passionne. Comme si tu parlais a un client au showroom.
- Tu CONNAIS les vehicules. Tu sais ce qui rend chaque modele special.
- Maximum 3-4 phrases pour l'intro. Pas de roman.
- Pas de hashtags dans l'intro.
- Pas d'emojis dans l'intro (ils viennent apres dans le corps de l'annonce)."""

        user_prompt = f"""Ecris une annonce Facebook pour ce vehicule:

VEHICULE: {ctx.get('title', '')}
PRIX: {ctx.get('price_formatted', '')}
KILOMETRAGE: {ctx.get('km_formatted', '')} ({ctx.get('km_description', '')})
POSITIONNEMENT PRIX: {ctx.get('price_description', '')}
TYPE: {vtype}

CONNAISSANCES SPECIFIQUES:
{chr(10).join(specs_info) if specs_info else "Aucune info specifique disponible."}

{f"SPECS DECODEES DU VIN (NHTSA):{chr(10)}{vin_specs_text}" if vin_specs_text else ""}

OPTIONS/EQUIPEMENTS CONFIRMES:
{chr(10).join(f"- {o}" for o in human_options) if human_options else "Aucune option confirmee."}

ANGLES DE VENTE SUGGERES: {', '.join(ctx.get('brand_angles', ['qualite', 'valeur', 'confiance'])[:3])}

INSTRUCTIONS:
1. Ecris une INTRO de 3-4 phrases maximum. Naturelle, directe, passionnee.
   - Mentionne ce qui rend CE vehicule special (pas une intro generique)
   - Si tu connais le moteur/HP, mentionne-le naturellement
   - Adapte le ton au type: {tone}

2. Puis le CORPS structure:
   - Titre avec le nom complet et l'annee
   - Prix
   - Kilometrage
   - Stock: {ctx.get('stock', '')}
   - 5-8 equipements/caracteristiques en points
   - Si c'est un Stellantis avec sticker: mention "Window Sticker verifie"

3. FERME avec: le nom Daniel Giroux et le numero 418-222-3939.
   Ne mets PAS "Kennebec Dodge" dans le footer (il est ajoute automatiquement).

FORMAT DE SORTIE: Texte pret a copier-coller sur Facebook. Utilise des emojis avec parcimonie dans le corps (pas dans l'intro).

STYLE D'INTRO: {style}"""

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return {"ok": False, "error": "OPENAI_API_KEY non configure", "intelligence": ctx}

        chat = AsyncOpenAI(api_key=api_key)
        _resp = await chat.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": user_prompt}],
            temperature=0.85, max_tokens=1500,
        )
        response = _resp.choices[0].message.content.strip()
        text = response.strip().strip('"').strip("'")

        # Post-process: remove cliches
        cliches = ["pret a dominer", "faire tourner les tetes", "sillonner la beauce",
                    "conquerir les chemins", "dominer les routes", "parcourir les routes de beauce",
                    "arpenter les routes", "routes de la beauce", "routes de beauce",
                    "chemins de la beauce", "paysages de la beauce"]
        for c in cliches:
            if c in text.lower():
                lines = text.split("\n")
                text = "\n".join(l for l in lines if c not in l.lower())

        # Post-process: remove vulgar/sexual words
        vulgar = ["couilles", "balls", "badass", "bitch", "cul ", "merde", "crisse", "tabarnac",
                  "calisse", "ostie", "fuck", "shit", "damn", "ass ", "sexy"]
        for v in vulgar:
            if v in text.lower():
                lines = text.split("\n")
                text = "\n".join(l for l in lines if v not in l.lower())

        return {
            "ok": True,
            "text": text.strip(),
            "intelligence": ctx,
            "vin_specs": vin_specs,
            "chars": len(text),
            "style": style,
            "model": "gpt-4o",
        }
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}

@api_router.post("/humanize-sticker/{stock}")
async def humanize_sticker_text(stock: str):
    """Humanise une annonce Stellantis existante (sticker_to_ad) avec l'IA."""
    if not sb:
        return {"ok": False, "error": "Supabase non connecte"}
    try:
        from vehicle_intelligence import build_vehicle_context
        from openai import AsyncOpenAI

        # Get vehicle info
        inv_result = sb.table("inventory").select("*").eq("stock", stock.upper()).limit(1).execute()
        vehicle = inv_result.data[0] if inv_result.data else {}
        ctx = build_vehicle_context(vehicle) if vehicle else {}

        # Get existing post with sticker text
        post_result = sb.table("posts").select("base_text,post_id").eq("stock", stock.upper()).limit(1).execute()
        if not post_result.data or not post_result.data[0].get("base_text"):
            return {"ok": False, "error": f"Aucun post avec sticker trouve pour {stock}"}

        base_text = post_result.data[0]["base_text"]
        has_sticker = "ACCESSOIRES" in base_text or "Window Sticker" in base_text
        if not has_sticker:
            return {"ok": False, "error": "Ce post ne contient pas de donnees Window Sticker", "is_sticker": False}

        title = vehicle.get("title", "") if vehicle else ""
        price = vehicle.get("price_int", 0) if vehicle else 0
        km = vehicle.get("km_int", 0) if vehicle else 0
        price_fmt = f"{price:,}".replace(",", " ") + " $" if price else ""
        km_fmt = f"{km:,}".replace(",", " ") + " km" if km else ""

        brand_identity = ctx.get("brand_identity", "") if ctx else ""
        model_known_for = ctx.get("model_known_for", "") if ctx else ""
        vtype = ctx.get("vehicle_type", "general") if ctx else "general"

        system_msg = """Tu es Daniel Giroux, vendeur passionne chez Kennebec Dodge Chrysler a Saint-Georges.
Tu recois une annonce Facebook generee a partir du Window Sticker d'un vehicule Stellantis.

TON TRAVAIL — Humaniser cette annonce en respectant ces regles STRICTES:

1. INTRO (3-4 phrases au debut):
   Ajoute une intro percutante, quebecoise, passionnee, specifique au vehicule.
   Pas de cliches, pas de vulgarite. Professionnel mais passionne.
   ABSOLUMENT AUCUN mot vulgaire, grossier ou a caractere sexuel.
   JAMAIS de "sillonner", "dominer", "Beauce", "routes de la Beauce" dans l'intro.

2. TITRE:
   Remplace SEULEMENT la premiere ligne (titre entre emojis) par un titre plus vendeur et humain.

3. OPTIONS — Structure STRICTE:
   ✅ = OPTIONS PRINCIPALES en MAJUSCULES humanisees (noms techniques traduits en francais lisible)
   ▫️ = sous-options en minuscules, plus discrets, en retrait
   
   IMPORTANT: 
   - NE SUPPRIME AUCUNE LIGNE. Chaque ✅ et ▫️ doit rester.
   - Les ✅ restent en MAJUSCULES. Noms humanises.
   - Les ▫️ sont en minuscules. Noms humanises.
   
   Exemples:
   - "COUCHE NACREE CRISTAL NOIR ETINCEL" → "PEINTURE NOIR CRISTAL NACREE"
   - "BANQ AVANT 40–20–40 TISSU CAT SUP" → "BANQUETTE AVANT 40/20/40 TISSU PREMIUM"
   - "SIEGE CONDUCT 10 REGL ELECT A/LOMB" → "siege conducteur 10 reglages electriques avec lombaire"
   - "SYST ELECTRO ANTIDERAPAGE" → "SYSTEME ANTIPATINAGE ELECTRONIQUE"
   - "TAPIS PROTECT AVANT/ARR T/S MOPARMD" → "TAPIS PROTECTEURS MOPAR AVANT/ARRIERE"
   - "ESSIEU ARR A/DIFFERENTIEL AUTOBLOQ" → "ESSIEU ARRIERE DIFFERENTIEL AUTOBLOQUANT"
   - "PLAQUE PROTECTION BOITE TRANSFERT" → "plaque de protection boite de transfert"

4. TOUT apres le lien sticker (footer echanges, Daniel Giroux, hashtags) = COPIE EXACTE, ne change RIEN.

NE RAJOUTE RIEN a la fin. Pas de commentaire, pas de "INFOS"."""

        prompt = f"""Humanise cette annonce:

{base_text}

INFOS: {title} | {price_fmt} | {km_fmt} | {vtype}
{f'Marque: {brand_identity}' if brand_identity else ''}
{f'Modele: {model_known_for}' if model_known_for else ''}"""

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            return {"ok": False, "error": "OPENAI_API_KEY non configure"}

        chat = AsyncOpenAI(api_key=api_key)
        _resp = await chat.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": prompt}],
            temperature=0.8, max_tokens=2000,
        )
        response = _resp.choices[0].message.content.strip()

        # Couper tout apres les hashtags
        lines = response.split("\n")
        output = []
        for line in lines:
            output.append(line)
            if line.strip().startswith("#") and "DanielGiroux" in line:
                break
        text = "\n".join(output).strip()

        # Filtre vulgarite
        vulgar = ["couilles", "balls", "badass", "bitch", "cul ", "merde", "crisse",
                  "tabarnac", "calisse", "ostie", "fuck", "shit", "damn", "ass ", "sexy"]
        for v in vulgar:
            if v in text.lower():
                text_lines = text.split("\n")
                text = "\n".join(l for l in text_lines if v not in l.lower())

        return {
            "ok": True,
            "text": text,
            "original": base_text,
            "intelligence": ctx,
            "chars": len(text),
            "is_sticker": True,
            "model": "gpt-4o",
        }
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}

@api_router.get("/test-batch-generate")
async def test_batch_generate(limit: int = 3):
    """Teste la generation sur plusieurs vehicules actifs."""
    if not sb:
        return {"ok": False, "error": "Supabase non connecte"}
    inv = sb.table("inventory").select("stock,title,price_int,km_int").eq("status", "ACTIVE").order("updated_at", desc=True).limit(limit).execute()
    if not inv.data:
        return {"ok": False, "error": "Aucun vehicule actif"}
    results = []
    for v in inv.data:
        stock = v.get("stock", "")
        try:
            from vehicle_intelligence import build_vehicle_context
            ctx = build_vehicle_context(v)
            results.append({
                "stock": stock,
                "title": v.get("title", ""),
                "parsing": {
                    "brand": ctx["brand"],
                    "model": ctx["model"],
                    "trim": ctx["trim"],
                    "type": ctx["vehicle_type"],
                    "hp": ctx["hp"],
                    "engine": ctx["engine"],
                    "vibe": ctx["trim_vibe"],
                    "km_desc": ctx["km_description"],
                    "price_desc": ctx["price_description"],
                },
            })
        except Exception as e:
            results.append({"stock": stock, "error": str(e)})
    return {"ok": True, "count": len(results), "vehicles": results}

@api_router.get("/architecture")
async def get_architecture():
    return ARCHITECTURE

# ═══════════════════════════════════════════════════
# COCKPIT — Simulation Dry Run + VIN Decode batch
# ═══════════════════════════════════════════════════

@api_router.get("/cockpit/decode-vin/{stock}")
async def cockpit_decode_vin(stock: str):
    """Decode le VIN d'un vehicule et retourne les specs NHTSA."""
    if not sb:
        return {"ok": False, "error": "Supabase non connecte"}
    try:
        from vin_decoder import decode_vin, format_specs_for_prompt, format_engine_line
        result = sb.table("inventory").select("stock,title,vin,price_int,km_int").eq("stock", stock.upper()).limit(1).execute()
        if not result.data:
            return {"ok": False, "error": f"Vehicule {stock} non trouve"}
        v = result.data[0]
        vin_val = (v.get("vin") or "").strip()
        if len(vin_val) < 11:
            return {"ok": False, "error": "VIN trop court ou absent", "vehicle": v}
        specs = decode_vin(vin_val)
        if not specs:
            return {"ok": False, "error": "NHTSA n'a pas retourne de donnees", "vehicle": v}
        return {
            "ok": True,
            "vehicle": v,
            "specs": specs,
            "engine_line": format_engine_line(specs),
            "specs_text": format_specs_for_prompt(specs),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@api_router.post("/cockpit/simulate")
async def cockpit_simulate(max_targets: int = 4, force_stock: Optional[str] = None):
    """Simule un dry run du cron: detecte les cibles et genere les textes SANS publier."""
    if not sb:
        return {"ok": False, "error": "Supabase non connecte"}

    from vehicle_intelligence import build_vehicle_context
    from openai import AsyncOpenAI
    import time as _time

    start = _time.time()
    results = []

    try:
        # Get active inventory
        inv_data = sb.table("inventory").select("*").eq("status", "ACTIVE").order("updated_at", desc=True).limit(200).execute()
        inventory = {v.get("stock", "").upper(): v for v in (inv_data.data or []) if v.get("stock")}

        # Get active posts
        posts_data = sb.table("posts").select("stock,post_id,base_text,published_at,no_photo,status").eq("status", "ACTIVE").limit(500).execute()
        posts_map = {(p.get("stock") or "").upper(): p for p in (posts_data.data or []) if p.get("stock")}

        # If force_stock, just simulate for that one
        if force_stock:
            stock = force_stock.strip().upper()
            if stock in inventory:
                v = inventory[stock]
                has_post = stock in posts_map
                event = "NEW" if not has_post else "UPDATE"
                res = await _simulate_one_vehicle(v, event, posts_map.get(stock))
                results.append(res)
            else:
                results.append({"stock": stock, "error": "Stock non trouve dans l'inventaire actif"})
        else:
            # Detect targets: vehicles without posts (NEW) or candidates for update
            new_targets = []
            for stock, v in inventory.items():
                if stock not in posts_map:
                    new_targets.append((stock, v, "NEW"))

            # Take top targets
            targets = new_targets[:max_targets]

            # If not enough new, pick some existing for preview
            if len(targets) < max_targets:
                existing = [(s, v, "PREVIEW") for s, v in list(inventory.items())[:max_targets * 2] if s in posts_map]
                targets.extend(existing[:max_targets - len(targets)])

            for stock, v, event in targets[:max_targets]:
                res = await _simulate_one_vehicle(v, event, posts_map.get(stock))
                results.append(res)

    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "traceback": traceback.format_exc()}

    elapsed = round(_time.time() - start, 1)
    return {
        "ok": True,
        "count": len(results),
        "elapsed_seconds": elapsed,
        "inventory_active": len(inventory) if 'inventory' in dir() else 0,
        "posts_active": len(posts_map) if 'posts_map' in dir() else 0,
        "results": results,
    }


async def _simulate_one_vehicle(v: Dict[str, Any], event: str, existing_post: Optional[Dict] = None) -> Dict[str, Any]:
    """Simule la generation de texte pour un vehicule."""
    from vehicle_intelligence import build_vehicle_context
    from openai import AsyncOpenAI
    import time as _time

    stock = (v.get("stock") or "").strip().upper()
    vin_val = (v.get("vin") or "").strip().upper()
    title = v.get("title", "")
    start = _time.time()

    result = {
        "stock": stock,
        "title": title,
        "vin": vin_val,
        "price": v.get("price_int"),
        "km": v.get("km_int"),
        "event": event,
        "has_existing_post": existing_post is not None,
        "generation_method": None,
        "text": None,
        "chars": 0,
        "vin_decoded": False,
        "vin_specs": None,
        "intelligence": None,
        "is_sticker": False,
        "error": None,
    }

    # 1. Vehicle Intelligence
    try:
        ctx = build_vehicle_context(v)
        result["intelligence"] = {
            "brand": ctx.get("brand"),
            "model": ctx.get("model"),
            "trim": ctx.get("trim"),
            "type": ctx.get("vehicle_type"),
            "hp": ctx.get("hp"),
            "engine": ctx.get("engine"),
            "vibe": ctx.get("trim_vibe"),
        }
    except Exception:
        ctx = {}

    # 2. VIN Decode
    vin_specs_text = ""
    if len(vin_val) >= 11:
        try:
            from vin_decoder import decode_vin, format_specs_for_prompt, format_engine_line
            specs = decode_vin(vin_val)
            if specs:
                result["vin_decoded"] = True
                result["vin_specs"] = {
                    "engine": format_engine_line(specs),
                    "drive": specs.get("drive_type", ""),
                    "transmission": specs.get("transmission", ""),
                    "fuel": specs.get("fuel_primary", ""),
                    "electrification": specs.get("electrification", ""),
                    "seats": specs.get("seats", ""),
                    "country": specs.get("plant_country", ""),
                }
                vin_specs_text = format_specs_for_prompt(specs)
                if not ctx.get("hp") and specs.get("engine_hp"):
                    result["intelligence"]["hp"] = specs["engine_hp"]
                    result["intelligence"]["engine"] = format_engine_line(specs).replace(f" — {specs['engine_hp']} HP", "")
        except Exception:
            pass

    # 3. Check if sticker post exists
    has_sticker = False
    sticker_text = ""
    if existing_post and existing_post.get("base_text"):
        bt = existing_post["base_text"]
        has_sticker = "ACCESSOIRES" in bt or "Window Sticker" in bt
        if has_sticker:
            sticker_text = bt
            result["is_sticker"] = True

    # 4. Generate text
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        result["error"] = "OPENAI_API_KEY manquant"
        return result

    try:
        if has_sticker and sticker_text:
            # Stellantis sticker humanization
            result["generation_method"] = "STICKER+AI"
            text = await _cockpit_humanize_sticker(api_key, sticker_text, v, ctx, vin_specs_text)
        else:
            # Standard generation with VIN
            result["generation_method"] = "LLM_V3+VIN" if vin_specs_text else "LLM_V3"
            text = await _cockpit_generate_text(api_key, v, ctx, vin_specs_text)

        if text:
            result["text"] = text
            result["chars"] = len(text)
        else:
            result["error"] = "Generation retourne vide"
    except Exception as e:
        result["error"] = str(e)

    result["elapsed"] = round(_time.time() - start, 1)
    return result


async def _cockpit_generate_text(api_key: str, v: Dict, ctx: Dict, vin_specs_text: str) -> str:
    """Genere un texte via OpenAI (meme prompts que generate-text endpoint)."""
    from openai import AsyncOpenAI
    from vehicle_intelligence import humanize_options
    import random

    vtype = ctx.get("vehicle_type", "general")
    tone_map = {
        "muscle_car": "adrenaline et son du moteur", "pickup": "robustesse et capacite",
        "pickup_hd": "robustesse et capacite", "off_road": "aventure et liberte",
        "suv_premium": "confort et raffinement", "citadine": "style et economie",
        "suv_compact": "style et economie", "exotique": "exclusivite et reve",
    }
    tone = tone_map.get(vtype, "polyvalence et fiabilite")
    style = random.choice(["direct", "storytelling", "question", "expertise", "opportunite"])

    specs_info = []
    if not vin_specs_text:
        if ctx.get("hp"):
            specs_info.append(f"Moteur: {ctx['engine']} — {ctx['hp']} chevaux")
    if ctx.get("trim_vibe"):
        specs_info.append(f"Ce trim: {ctx['trim_vibe']}")
    if ctx.get("model_known_for"):
        specs_info.append(f"Ce modele: {ctx['model_known_for']}")

    system_msg = "Tu es Daniel Giroux, vendeur passionne chez Kennebec Dodge Chrysler a Saint-Georges.\nREGLES: Francais quebecois naturel. Passionne mais professionnel. AUCUN mot vulgaire. JAMAIS mentionner la Beauce. Max 3-4 phrases d'intro. Pas de cliches."

    price = v.get("price_int", 0)
    km = v.get("km_int", 0)
    prompt = f"""Ecris une annonce Facebook:
VEHICULE: {v.get('title','')}
PRIX: {f'{price:,}'.replace(',', ' ')} $ | KM: {f'{km:,}'.replace(',', ' ')} km
STOCK: {v.get('stock','')}
TYPE: {vtype} | TON: {tone} | STYLE: {style}
{chr(10).join(specs_info) if specs_info else ''}
{f'SPECS VIN:{chr(10)}{vin_specs_text}' if vin_specs_text else ''}
Intro 3-4 phrases + corps structure + Daniel Giroux 418-222-3939"""

    client = AsyncOpenAI(api_key=api_key)
    _resp = await client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": prompt}],
        temperature=0.85, max_tokens=1500,
    )
    return _resp.choices[0].message.content.strip()


async def _cockpit_humanize_sticker(api_key: str, sticker_text: str, v: Dict, ctx: Dict, vin_specs_text: str) -> str:
    """Humanise un texte sticker via OpenAI."""
    from openai import AsyncOpenAI

    system_msg = (
        "Tu es Daniel Giroux, vendeur passionne chez Kennebec Dodge Chrysler.\n"
        "Humanise cette annonce sticker:\n"
        "1. INTRO 3-4 phrases passionnees. AUCUN mot vulgaire. JAMAIS 'Beauce'.\n"
        "2. TITRE vendeur au lieu du titre brut.\n"
        "3. OPTIONS: ✅ MAJUSCULES humanisees, ▫️ minuscules. NE SUPPRIME AUCUNE LIGNE.\n"
        "4. Apres le lien sticker: COPIE EXACTE du footer.\n"
        "NE RAJOUTE RIEN a la fin."
    )
    prompt = f"Humanise:\n\n{sticker_text}"
    if vin_specs_text:
        prompt += f"\n\nSPECS VIN:\n{vin_specs_text}"

    client = AsyncOpenAI(api_key=api_key)
    _resp = await client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": prompt}],
        temperature=0.8, max_tokens=2000,
    )
    resp = _resp.choices[0].message.content.strip()

    # Couper après les hashtags
    lines = resp.split("\n")
    output = []
    for line in lines:
        output.append(line)
        if line.strip().startswith("#") and "DanielGiroux" in line:
            break
    return "\n".join(output).strip()


@api_router.get("/cockpit/recent-logs")
async def cockpit_recent_logs(limit: int = 30):
    """Retourne les events recents groupes par run."""
    if not sb:
        return {"ok": False, "error": "Supabase non connecte"}
    events = sb_query("events", "*", order="created_at", limit=limit)
    runs = sb_query("scrape_runs", "*", order="created_at", limit=5)
    return {
        "ok": True,
        "events": events["data"],
        "runs": runs["data"],
    }

# ═══════════════════════════════════════════════════
# REPRISE — Vehicle Appraisal Endpoints
# ═══════════════════════════════════════════════════

ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "4182223939")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Daniel7$")
REPRISE_STORAGE_BUCKET = "reprise-photos"
VIN_DECODE_CACHE = {}

# Fallback users if Supabase table not created yet
FALLBACK_USERS = {
    "admin": {"password": "Daniel7$", "name": "Daniel Giroux", "role": "admin"},
    "directeur": {"password": "Ventes2025!", "name": "Directeur des ventes", "role": "directeur"},
}

def get_dashboard_users():
    if sb:
        try:
            result = sb.table("dashboard_users").select("*").eq("active", True).execute()
            if result.data:
                return {u["username"]: u for u in result.data}
        except Exception as e:
            logging.warning(f"dashboard_users table not found, using fallback: {e}")
    return FALLBACK_USERS

def decode_vin_nhtsa(vin: str) -> dict:
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return {}
    if vin in VIN_DECODE_CACHE:
        return VIN_DECODE_CACHE[vin]
    try:
        r = http_req.get(f"https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvalues/{vin}?format=json", timeout=15)
        if r.ok:
            results = r.json().get("Results", [{}])[0]
            specs = {
                "make": results.get("Make", ""), "model": results.get("Model", ""),
                "year": results.get("ModelYear", ""), "trim": results.get("Trim", ""),
                "body": results.get("BodyClass", ""), "engine_cylinders": results.get("EngineCylinders", ""),
                "engine_displacement": results.get("DisplacementL", ""), "engine_hp": results.get("EngineHP", ""),
                "fuel_type": results.get("FuelTypePrimary", ""), "transmission": results.get("TransmissionStyle", ""),
                "drive_type": results.get("DriveType", ""), "doors": results.get("Doors", ""),
            }
            specs = {k: v for k, v in specs.items() if v and str(v).strip()}
            VIN_DECODE_CACHE[vin] = specs
            return specs
    except Exception as e:
        logging.error(f"VIN decode error: {e}")
    return {}

@api_router.get("/vin/{vin}")
async def reprise_decode_vin(vin: str):
    from fastapi import HTTPException
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        raise HTTPException(400, "VIN doit faire exactement 17 caracteres")
    specs = decode_vin_nhtsa(vin)
    if not specs:
        raise HTTPException(404, "VIN non trouve")

    # Enhanced trim detection via AI if NHTSA gives multiple trims
    trim_val = specs.get("trim", "")
    if trim_val and ("," in trim_val or "/" in trim_val or len(trim_val) > 30):
        try:
            from emergentintegrations.llm.chat import LlmChat, UserMessage
            api_key = os.environ.get("EMERGENT_LLM_KEY", "")
            if api_key:
                chat = LlmChat(api_key=api_key, session_id=f"vin-trim-{vin[-6:]}", system_message="Tu es un expert en decodage VIN automobile. Reponds UNIQUEMENT avec le nom du trim exact, rien d'autre.")
                chat.with_model("openai", "gpt-4o")
                prompt = f"VIN: {vin}\nMarque: {specs.get('make','')}\nModele: {specs.get('model','')}\nAnnee: {specs.get('year','')}\nTrims possibles (NHTSA): {trim_val}\nMoteur: {specs.get('engine_cylinders','')}cyl {specs.get('engine_displacement','')}L\nCarburant: {specs.get('fuel_type','')}\n\nQuel est le trim EXACT? Pour les vehicules au Canada, utilise les noms canadiens (ex: VW = Trendline/Comfortline/Highline au lieu de S/SE/SEL)."
                ai_trim = await chat.send_message(UserMessage(text=prompt))
                ai_trim = ai_trim.strip().strip('"').strip("'")
                if ai_trim and len(ai_trim) < 50:
                    specs["trim"] = ai_trim
                    specs["trim_source"] = "ai_enhanced"
        except Exception as e:
            logging.warning(f"AI trim decode failed: {e}")

    return {"vin": vin, "specs": specs}

@api_router.post("/vin/scan-photo")
async def reprise_scan_vin_photo(file: UploadFile = File(...)):
    from fastapi import HTTPException
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "Image trop grande (max 10MB)")
    api_key = os.environ.get("EMERGENT_LLM_KEY", "")
    if not api_key:
        raise HTTPException(500, "Cle IA non configuree")
    import base64
    b64 = base64.b64encode(content).decode("utf-8")
    try:
        from emergentintegrations.llm.chat import LlmChat, UserMessage, ImageContent
        chat = LlmChat(api_key=api_key, session_id=f"vin-scan-{uuid.uuid4().hex[:8]}", system_message="Tu extrais le numero VIN (17 caracteres alphanumeriques, pas de I, O, Q) depuis des photos. Reponds UNIQUEMENT avec le VIN ou 'ERREUR' si illisible.")
        chat.with_model("openai", "gpt-4o")
        response = await chat.send_message(UserMessage(text="Lis le VIN sur cette image. Reponds UNIQUEMENT le VIN.", file_contents=[ImageContent(image_base64=b64)]))
        result = response.strip().upper().replace(" ", "").replace("-", "")
        if result.startswith("ERREUR"):
            return {"success": False, "error": result}
        clean = "".join(c for c in result if c.isalnum())[:17]
        if len(clean) == 17:
            specs = decode_vin_nhtsa(clean)
            return {"success": True, "vin": clean, "specs": specs}
        return {"success": False, "error": f"VIN illisible ({len(clean)} car.)", "partial": clean}
    except Exception as e:
        logging.error(f"VIN scan error: {e}")
        raise HTTPException(500, str(e))

@api_router.post("/reprise/auth/login")
async def reprise_login(data: dict):
    from fastapi import HTTPException
    username = (data.get("username") or "").strip().lower()
    password = (data.get("password") or "").strip()
    users = get_dashboard_users()

    if username in users:
        u = users[username]
        if u["password"] == password:
            import hashlib
            token = hashlib.sha256(f"{username}:{password}:{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:32]
            return {"success": True, "token": token, "name": u["name"], "role": u["role"], "username": username}

    raise HTTPException(401, "Identifiants incorrects")


@api_router.get("/users")
async def list_users():
    users = get_dashboard_users()
    return {"users": [{"username": k, "name": v["name"], "role": v["role"], "email": v.get("email", "")} for k, v in users.items()]}


@api_router.post("/users/change-password")
async def change_password(data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    username = (data.get("username") or "").strip().lower()
    old_password = (data.get("old_password") or "").strip()
    new_password = (data.get("new_password") or "").strip()
    if not username or not old_password or not new_password:
        raise HTTPException(400, "username, old_password et new_password requis")
    if len(new_password) < 6:
        raise HTTPException(400, "Le nouveau mot de passe doit avoir au moins 6 caracteres")
    users = get_dashboard_users()
    if username not in users or users[username]["password"] != old_password:
        raise HTTPException(401, "Mot de passe actuel incorrect")
    try:
        sb.table("dashboard_users").update({"password": new_password}).eq("username", username).execute()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))


@api_router.post("/users/forgot-password")
async def forgot_password(data: dict):
    from fastapi import HTTPException
    import random, string
    if not sb:
        raise HTTPException(500, "DB non connectee")
    username = (data.get("username") or "").strip().lower()
    if not username:
        raise HTTPException(400, "username requis")
    users = get_dashboard_users()
    if username not in users:
        return {"success": True, "message": "Si ce compte existe, un courriel a ete envoye."}
    u = users[username]
    email = u.get("email", "")
    if not email:
        return {"success": False, "message": "Aucun courriel associe a ce compte. Contactez l'administrateur."}
    temp_pwd = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    try:
        sb.table("dashboard_users").update({"password": temp_pwd}).eq("username", username).execute()
        body = f"""
        <div style="font-family:Arial,sans-serif;max-width:500px;margin:0 auto;padding:2rem">
          <h2 style="color:#0284c7">Reinitialisation de mot de passe</h2>
          <p>Bonjour <strong>{u.get('name', username)}</strong>,</p>
          <p>Votre mot de passe temporaire est:</p>
          <div style="background:#f0f9ff;border:2px solid #0284c7;border-radius:8px;padding:1rem;text-align:center;margin:1rem 0">
            <span style="font-family:monospace;font-size:1.5rem;font-weight:700;color:#0284c7;letter-spacing:2px">{temp_pwd}</span>
          </div>
          <p>Connectez-vous et changez votre mot de passe immediatement.</p>
          <p style="color:#6b7280;font-size:0.85em">Kennebec Dodge Chrysler — 418-222-3939</p>
        </div>"""
        send_email(email, "Reinitialisation mot de passe — Kenbot", body)
        return {"success": True, "message": "Un courriel avec votre nouveau mot de passe a ete envoye."}
    except Exception as e:
        raise HTTPException(500, str(e))


@api_router.post("/users")
async def create_user(data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    username = (data.get("username") or "").strip().lower()
    password = (data.get("password") or "").strip()
    name = (data.get("name") or "").strip()
    role = data.get("role", "conseiller")
    if not username or not password or not name:
        raise HTTPException(400, "Champs requis: username, password, name")
    if role not in ("admin", "directeur", "conseiller"):
        raise HTTPException(400, "Role invalide")
    try:
        sb.table("dashboard_users").insert({"username": username, "password": password, "name": name, "role": role, "email": (data.get("email") or "").strip()}).execute()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))


@api_router.patch("/users/{username}")
async def update_user(username: str, data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    allowed = {"password", "name", "role", "active", "email"}
    update = {k: v for k, v in data.items() if k in allowed}
    if not update:
        raise HTTPException(400, "Rien a modifier")
    try:
        sb.table("dashboard_users").update(update).eq("username", username).execute()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))


@api_router.delete("/users/{username}")
async def delete_user(username: str):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    if username == "admin":
        raise HTTPException(400, "Impossible de supprimer l'admin")
    try:
        sb.table("dashboard_users").delete().eq("username", username).execute()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))

@api_router.post("/evaluations")
async def reprise_create_evaluation(data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "Base de donnees non connectee")
    vin = (data.get("vin") or "").strip().upper()
    specs = decode_vin_nhtsa(vin) if len(vin) == 17 else {}
    engine_parts = [specs.get("engine_cylinders", ""), "cyl", specs.get("engine_displacement", ""), "L", specs.get("engine_hp", ""), "HP"]
    engine_str = " ".join(p for p in engine_parts if p).strip()
    evaluation = {
        "id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "NOUVEAU",
        "created_by": (data.get("created_by") or "client").strip(),
        "client_name": f"{(data.get('prenom') or '').strip()} {(data.get('nom') or '').strip()}".strip(),
        "client_phone": (data.get("telephone") or "").strip(),
        "client_email": (data.get("courriel") or "").strip(),
        "client_notes": (data.get("notes_client") or "").strip(),
        "vin": vin, "make": specs.get("make", ""), "model": specs.get("model", ""),
        "year": specs.get("year", ""), "trim": specs.get("trim", ""),
        "engine": engine_str if engine_str != "cyl L HP" else "",
        "drive_type": specs.get("drive_type", ""), "fuel_type": specs.get("fuel_type", ""),
        "km": data.get("km"), "paiement_restant": data.get("paiement_restant"),
        "etat_general": data.get("etat_general", ""),
        "photos": data.get("photos", []), "vin_decoded": specs,
        "form_data": {k: v for k, v in data.items() if k not in ("photos",)},
    }
    try:
        sb.table("evaluations").insert(evaluation).execute()
        return {"success": True, "id": evaluation["id"]}
    except Exception as e:
        logging.error(f"Insert evaluation error: {e}")
        raise HTTPException(500, str(e))

@api_router.get("/evaluations")
async def reprise_list_evaluations(created_by: str = None, role: str = None):
    if not sb:
        return {"evaluations": []}
    try:
        q = sb.table("evaluations").select("*").order("created_at", desc=True).limit(200)
        # Conseillers only see their own evaluations
        if role == "conseiller" and created_by:
            q = q.eq("created_by", created_by)
        result = q.execute()
        return {"evaluations": result.data or []}
    except Exception as e:
        logging.error(f"List evaluations error: {e}")
        return {"evaluations": [], "error": str(e)}

@api_router.get("/evaluations/{eval_id}")
async def reprise_get_evaluation(eval_id: str):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    try:
        result = sb.table("evaluations").select("*").eq("id", eval_id).limit(1).execute()
        if not result.data:
            raise HTTPException(404, "Evaluation non trouvee")
        return {"evaluation": result.data[0]}
    except Exception as e:
        raise HTTPException(500, str(e))

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER)

def send_email(to_email, subject, body_html, reply_to=None):
    if not SMTP_USER or not SMTP_PASS:
        logging.warning("SMTP not configured, skipping email")
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"Kennebec Reprise <{SMTP_FROM}>"
        msg["To"] = to_email
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_FROM, to_email, msg.as_string())
        logging.info(f"Email sent to {to_email}: {subject}")
        return True
    except Exception as e:
        logging.error(f"Email error: {e}")
        return False


@api_router.patch("/evaluations/{eval_id}")
async def reprise_update_evaluation(eval_id: str, data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    allowed = {"status", "admin_notes", "offre_montant", "prix_reprise", "prix_par", "wholesale_emails"}
    update = {k: v for k, v in data.items() if k in allowed}
    update["updated_at"] = datetime.now(timezone.utc).isoformat()

    # If directeur sets a price, auto-change status and notify
    prix = data.get("prix_reprise")
    prix_par = data.get("prix_par", "")
    notify_email = data.get("notify_email", "")

    if prix and prix_par:
        update["status"] = "PRIX RECU"
        update["prix_reprise"] = prix
        update["prix_par"] = prix_par
        update["prix_date"] = datetime.now(timezone.utc).isoformat()

    try:
        sb.table("evaluations").update(update).eq("id", eval_id).execute()

        # Send email notification to conseiller if price was set
        if prix and notify_email:
            result = sb.table("evaluations").select("*").eq("id", eval_id).limit(1).execute()
            ev = result.data[0] if result.data else {}
            subject = f"Prix de reprise recu — {ev.get('year','')} {ev.get('make','')} {ev.get('model','')}"
            body = f"""
            <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
              <h2 style="color:#0ea5e9">Prix de reprise recu</h2>
              <p>Le directeur <strong>{prix_par}</strong> a evalue le vehicule:</p>
              <table style="width:100%;border-collapse:collapse;margin:1rem 0">
                <tr><td style="padding:8px;border-bottom:1px solid #eee;color:#666">Vehicule</td><td style="padding:8px;border-bottom:1px solid #eee;font-weight:bold">{ev.get('year','')} {ev.get('make','')} {ev.get('model','')} {ev.get('trim','')}</td></tr>
                <tr><td style="padding:8px;border-bottom:1px solid #eee;color:#666">VIN</td><td style="padding:8px;border-bottom:1px solid #eee;font-family:monospace">{ev.get('vin','')}</td></tr>
                <tr><td style="padding:8px;border-bottom:1px solid #eee;color:#666">Client</td><td style="padding:8px;border-bottom:1px solid #eee">{ev.get('client_name','')}</td></tr>
                <tr><td style="padding:8px;border-bottom:1px solid #eee;color:#666">KM</td><td style="padding:8px;border-bottom:1px solid #eee">{ev.get('km','')} km</td></tr>
                <tr style="background:#f0f9ff"><td style="padding:12px;color:#0ea5e9;font-weight:bold">PRIX DE REPRISE</td><td style="padding:12px;font-size:1.3em;font-weight:bold;color:#0ea5e9">{prix} $</td></tr>
              </table>
              <p style="color:#666;font-size:0.85em">Kennebec Dodge Chrysler — 418-222-3939</p>
            </div>"""
            send_email(notify_email, subject, body)

        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))

@api_router.post("/evaluations/upload-photo")
async def reprise_upload_photo(file: UploadFile = File(...), evaluation_id: str = FastForm("")):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "Max 10MB")
    ext = file.filename.split(".")[-1] if file.filename and "." in file.filename else "jpg"
    path = f"{evaluation_id or 'temp'}/{uuid.uuid4().hex[:8]}.{ext}"
    try:
        sb.storage.from_(REPRISE_STORAGE_BUCKET).upload(path, content, {"content-type": file.content_type or "image/jpeg"})
        public_url = f"{SUPABASE_URL}/storage/v1/object/public/{REPRISE_STORAGE_BUCKET}/{path}"
        return {"success": True, "url": public_url, "path": path}
    except Exception as e:
        raise HTTPException(500, str(e))

# ─── Health Check ───
@api_router.get("/health")
async def health_check():
    status = {
        "status": "ok",
        "service": "kenbot-dashboard-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "supabase": "connected" if sb else "disconnected"
    }
    return status

app.include_router(api_router)

# ═══ WHOLESALE ENDPOINTS ═══
wholesale_router = APIRouter(prefix="/api")

@wholesale_router.get("/wholesale-contacts")
async def list_wholesale_contacts():
    if not sb:
        return {"contacts": []}
    try:
        result = sb.table("wholesale_contacts").select("*").eq("active", True).order("name").execute()
        return {"contacts": result.data or []}
    except Exception as e:
        logging.warning(f"wholesale_contacts table may not exist: {e}")
        return {"contacts": []}

@wholesale_router.post("/wholesale-contacts")
async def add_wholesale_contact(data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    contact = {
        "id": str(uuid.uuid4()),
        "name": (data.get("name") or "").strip(),
        "email": (data.get("email") or "").strip(),
        "phone": (data.get("phone") or "").strip(),
        "active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        sb.table("wholesale_contacts").insert(contact).execute()
        return {"success": True, "id": contact["id"]}
    except Exception as e:
        raise HTTPException(500, str(e))

@wholesale_router.delete("/wholesale-contacts/{contact_id}")
async def delete_wholesale_contact(contact_id: str):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    try:
        sb.table("wholesale_contacts").delete().eq("id", contact_id).execute()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))

@wholesale_router.post("/wholesale/send")
async def send_wholesale_email(data: dict):
    from fastapi import HTTPException
    if not sb:
        raise HTTPException(500, "DB non connectee")
    eval_id = data.get("evaluation_id")
    contact_email = data.get("contact_email")
    contact_name = data.get("contact_name", "")
    reply_to = data.get("reply_to", "")
    if not eval_id or not contact_email:
        raise HTTPException(400, "evaluation_id et contact_email requis")
    try:
        result = sb.table("evaluations").select("*").eq("id", eval_id).limit(1).execute()
        if not result.data:
            raise HTTPException(404, "Evaluation non trouvee")
        ev = result.data[0]
        fd = ev.get("form_data", {})
        photos_html = ""
        for p in (ev.get("photos") or [])[:5]:
            photos_html += f'<img src="{p}" style="width:150px;height:120px;object-fit:cover;border-radius:6px;margin:4px" />'
        options_html = ""
        for o in (fd.get("options") or []):
            options_html += f'<span style="display:inline-block;padding:2px 8px;margin:2px;border-radius:4px;background:#f0f9ff;color:#0284c7;font-size:12px">{o}</span>'
        subject = f"Evaluation de reprise — {ev.get('year','')} {ev.get('make','')} {ev.get('model','')} {ev.get('trim','')}"
        # Get directeur info for reply-to
        director_line = "Daniel Giroux — 418-222-3939"
        if reply_to:
            director_line = f"{reply_to}"
        body = f"""
        <div style="font-family:Arial,sans-serif;max-width:650px;margin:0 auto;background:#f9fafb;padding:2rem;border-radius:10px">
          <div style="text-align:center;margin-bottom:1.5rem">
            <h2 style="color:#0284c7;margin:0">Evaluation de reprise</h2>
            <p style="color:#6b7280;margin:0.25rem 0">Kennebec Dodge Chrysler — Saint-Georges</p>
          </div>
          <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:1.5rem;margin-bottom:1rem">
            <h3 style="margin:0 0 0.5rem;color:#111">{ev.get('year','')} {ev.get('make','')} {ev.get('model','')} {ev.get('trim','')}</h3>
            <table style="width:100%;border-collapse:collapse">
              <tr><td style="padding:6px 0;color:#6b7280;width:120px">VIN</td><td style="padding:6px 0;font-family:monospace">{ev.get('vin','')}</td></tr>
              <tr><td style="padding:6px 0;color:#6b7280">Kilometrage</td><td style="padding:6px 0;font-weight:600">{ev.get('km','')} km</td></tr>
              <tr><td style="padding:6px 0;color:#6b7280">Moteur</td><td style="padding:6px 0">{ev.get('engine','')}</td></tr>
              <tr><td style="padding:6px 0;color:#6b7280">Motricite</td><td style="padding:6px 0">{ev.get('drive_type','')}</td></tr>
              <tr><td style="padding:6px 0;color:#6b7280">Etat general</td><td style="padding:6px 0;font-weight:600">{ev.get('etat_general','')}</td></tr>
              <tr><td style="padding:6px 0;color:#6b7280">Couleur ext.</td><td style="padding:6px 0">{fd.get('couleur_ext','')}</td></tr>
            </table>
          </div>
          {f'<div style="margin-bottom:1rem">{photos_html}</div>' if photos_html else ''}
          {f'<div style="margin-bottom:1rem"><strong>Equipements:</strong><br/>{options_html}</div>' if options_html else ''}
          <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:1rem;text-align:center">
            <p style="color:#6b7280;margin:0 0 0.5rem;font-size:14px">Interesse? Repondez directement a ce courriel.</p>
            <p style="color:#111;font-weight:600;margin:0">Kennebec Dodge Chrysler — 418-222-3939</p>
          </div>
        </div>"""
        send_email(contact_email, subject, body, reply_to=reply_to or None)
        return {"success": True}
    except Exception as e:
        logging.error(f"Wholesale send error: {e}")
        raise HTTPException(500, str(e))

app.include_router(wholesale_router)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup():
    if sb:
        logger.info(f"Supabase connected to {SUPABASE_URL}")
    else:
        logger.warning("Supabase NOT connected - dashboard will show no data")
