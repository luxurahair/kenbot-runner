from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import os
import logging
import json
import hashlib
import uuid
from datetime import datetime, timezone

# ── Supabase ──
try:
    from supabase import create_client, Client
except ImportError:
    create_client = None
    Client = None

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Kenbot Reprise API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Supabase connection ──
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "4182223939")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "Liana2018$")
STORAGE_BUCKET = "reprise-photos"

sb: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY and create_client:
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Supabase connected")
    except Exception as e:
        logging.error(f"Supabase error: {e}")

# ── VIN Decoder (NHTSA) ──
import requests as req

VIN_CACHE = {}

def decode_vin(vin: str) -> dict:
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        return {}
    if vin in VIN_CACHE:
        return VIN_CACHE[vin]
    try:
        r = req.get(
            f"https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvalues/{vin}?format=json",
            timeout=15,
        )
        if r.ok:
            results = r.json().get("Results", [{}])[0]
            specs = {
                "make": results.get("Make", ""),
                "model": results.get("Model", ""),
                "year": results.get("ModelYear", ""),
                "trim": results.get("Trim", ""),
                "body": results.get("BodyClass", ""),
                "engine_cylinders": results.get("EngineCylinders", ""),
                "engine_displacement": results.get("DisplacementL", ""),
                "engine_hp": results.get("EngineHP", ""),
                "fuel_type": results.get("FuelTypePrimary", ""),
                "transmission": results.get("TransmissionStyle", ""),
                "drive_type": results.get("DriveType", ""),
                "doors": results.get("Doors", ""),
                "plant_country": results.get("PlantCountry", ""),
                "plant_city": results.get("PlantCity", ""),
            }
            # Clean empty values
            specs = {k: v for k, v in specs.items() if v and str(v).strip()}
            VIN_CACHE[vin] = specs
            return specs
    except Exception as e:
        logging.error(f"VIN decode error: {e}")
    return {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Routes ──

@app.get("/api/")
async def root():
    return {"app": "Kenbot Reprise", "version": "1.0.0", "supabase": sb is not None}


@app.get("/api/vin/{vin}")
async def decode_vin_route(vin: str):
    """Decode VIN via NHTSA — retourne les specs du véhicule."""
    vin = (vin or "").strip().upper()
    if len(vin) != 17:
        raise HTTPException(400, "VIN doit faire exactement 17 caractères")
    specs = decode_vin(vin)
    if not specs:
        raise HTTPException(404, "VIN non trouvé dans la base NHTSA")
    return {"vin": vin, "specs": specs}


@app.post("/api/auth/login")
async def admin_login(data: dict):
    """Login admin — vérifie téléphone + mot de passe."""
    phone = (data.get("phone") or "").replace("-", "").replace(" ", "").replace("(", "").replace(")", "").strip()
    password = (data.get("password") or "").strip()

    if phone == ADMIN_PHONE and password == ADMIN_PASSWORD:
        token = hashlib.sha256(f"{phone}:{password}:{utc_now_iso()}".encode()).hexdigest()[:32]
        return {"success": True, "token": token, "name": "Daniel Giroux"}
    raise HTTPException(401, "Identifiants incorrects")


@app.post("/api/evaluations")
async def create_evaluation(data: dict):
    """Créer une nouvelle évaluation (soumission client)."""
    if not sb:
        raise HTTPException(500, "Base de données non connectée")

    vin = (data.get("vin") or "").strip().upper()
    specs = decode_vin(vin) if len(vin) == 17 else {}

    evaluation = {
        "id": str(uuid.uuid4()),
        "created_at": utc_now_iso(),
        "status": "NOUVEAU",
        "client_name": (data.get("client_name") or "").strip(),
        "client_phone": (data.get("client_phone") or "").strip(),
        "client_email": (data.get("client_email") or "").strip(),
        "client_notes": (data.get("client_notes") or "").strip(),
        "vin": vin,
        "make": specs.get("make", ""),
        "model": specs.get("model", ""),
        "year": specs.get("year", ""),
        "trim": specs.get("trim", ""),
        "engine": f"{specs.get('engine_cylinders', '')}cyl {specs.get('engine_displacement', '')}L {specs.get('engine_hp', '')}HP".strip(),
        "drive_type": specs.get("drive_type", ""),
        "fuel_type": specs.get("fuel_type", ""),
        "km": data.get("km"),
        "paiement_restant": data.get("paiement_restant"),
        "etat_general": data.get("etat_general", ""),
        "photos": data.get("photos", []),
        "vin_decoded": specs,
    }

    try:
        sb.table("evaluations").insert(evaluation).execute()
        return {"success": True, "id": evaluation["id"]}
    except Exception as e:
        logging.error(f"Insert evaluation error: {e}")
        raise HTTPException(500, str(e))


@app.post("/api/evaluations/upload-photo")
async def upload_photo(file: UploadFile = File(...), evaluation_id: str = Form("")):
    """Upload une photo dans Supabase Storage."""
    if not sb:
        raise HTTPException(500, "Base de données non connectée")

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "Photo trop grande (max 10MB)")

    ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
    path = f"{evaluation_id or 'temp'}/{uuid.uuid4().hex[:8]}.{ext}"

    try:
        sb.storage.from_(STORAGE_BUCKET).upload(path, content, {"content-type": file.content_type or "image/jpeg"})
        public_url = f"{SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}/{path}"
        return {"success": True, "url": public_url, "path": path}
    except Exception as e:
        logging.error(f"Upload error: {e}")
        raise HTTPException(500, str(e))


@app.get("/api/evaluations")
async def list_evaluations():
    """Liste toutes les évaluations (admin)."""
    if not sb:
        return {"evaluations": []}
    try:
        result = sb.table("evaluations").select("*").order("created_at", desc=True).limit(200).execute()
        return {"evaluations": result.data or []}
    except Exception as e:
        logging.error(f"List evaluations error: {e}")
        return {"evaluations": [], "error": str(e)}


@app.get("/api/evaluations/{eval_id}")
async def get_evaluation(eval_id: str):
    """Détail d'une évaluation."""
    if not sb:
        raise HTTPException(500, "Base de données non connectée")
    try:
        result = sb.table("evaluations").select("*").eq("id", eval_id).limit(1).execute()
        if not result.data:
            raise HTTPException(404, "Évaluation non trouvée")
        return {"evaluation": result.data[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.patch("/api/evaluations/{eval_id}")
async def update_evaluation(eval_id: str, data: dict):
    """Mettre à jour le statut d'une évaluation (admin)."""
    if not sb:
        raise HTTPException(500, "Base de données non connectée")

    allowed = {"status", "admin_notes", "offre_montant"}
    update = {k: v for k, v in data.items() if k in allowed}
    update["updated_at"] = utc_now_iso()

    try:
        sb.table("evaluations").update(update).eq("id", eval_id).execute()
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))
