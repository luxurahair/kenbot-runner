"""
pipeline/generator.py — Fonctions de generation unifiees.
Point d'entree central pour toute generation de texte AI.
Delegue au client centralise et utilise les prompts centralises.
"""

import random
from typing import Dict, Any, Optional

from pipeline.client import chat_completion, get_default_model, get_smart_model
from pipeline.cliches import filter_cliches, remove_cliche_lines
from pipeline.prompts import (
    build_accroche_system_prompt,
    build_smart_prompt,
    build_humanize_prompt,
    build_intro_prompt,
    SYSTEM_PROMPT_V3,
    ANGLES_PAR_TYPE,
)


# ============================================================
# HELPERS
# ============================================================

def _fmt_money(value) -> str:
    if value in (None, ""):
        return ""
    try:
        n = int(float(value))
        return f"{n:,}".replace(",", " ") + " $"
    except Exception:
        return str(value).strip()


def _fmt_km(value) -> str:
    if value in (None, ""):
        return ""
    try:
        n = int(float(value))
        return f"{n:,}".replace(",", " ") + " km"
    except Exception:
        return str(value).strip()


def _vehicle_price(vehicle: Dict[str, Any]) -> str:
    v = vehicle or {}
    raw = v.get("price")
    if raw in (None, ""):
        raw = v.get("price_int")
    return _fmt_money(raw)


def _vehicle_mileage(vehicle: Dict[str, Any]) -> str:
    v = vehicle or {}
    raw = v.get("mileage")
    if raw in (None, ""):
        raw = v.get("km")
    if raw in (None, ""):
        raw = v.get("km_int")
    return _fmt_km(raw)


def _get_features_text(vehicle: Dict[str, Any]) -> str:
    features = vehicle.get("features") or []
    comfort = vehicle.get("comfort") or []
    all_features = features + comfort
    if not all_features:
        return "Non specifies"
    return ", ".join(str(f) for f in all_features[:5])


def _safe_trim(text: str, max_chars: int) -> str:
    txt = (text or "").strip()
    if len(txt) <= max_chars:
        return txt
    phone = "418-222-3939"
    if phone in txt:
        idx = txt.find(phone) + len(phone)
        if idx <= max_chars:
            return txt[:idx].rstrip(" .,!?;:-")
    cut = txt[:max_chars].rstrip(" .,!?;:-")
    return cut + "..."


# ============================================================
# GENERATION V2 (llm.py compatible)
# ============================================================

def generate_ad_text(
    vehicle: Dict[str, Any],
    kind: str = "default",
    max_chars: int = 400,
) -> str:
    """
    Genere une accroche AI Facebook variee, centree sur Daniel Giroux.
    Compatible avec llm.py v2.
    """
    try:
        from classifier import classify
    except ImportError:
        def classify(v):
            return "default"

    v = vehicle or {}
    title = (v.get("title") or "Vehicule").strip()
    price = _vehicle_price(v)
    mileage = _vehicle_mileage(v)
    stock = str(v.get("stock") or "").strip()
    features = _get_features_text(v)

    vehicle_type = classify(v) if kind == "default" else kind
    if vehicle_type == "price_changed":
        vehicle_type = classify(v)

    # Construire l'angle selon l'event
    if kind == "price_changed":
        old_price = _fmt_money(v.get("old_price"))
        new_price = _fmt_money(v.get("new_price"))
        event_angle = f"""
EVENEMENT: BAISSE DE PRIX
- Ancien prix: {old_price}
- Nouveau prix: {new_price}
- Fais sentir l'opportunite avec un ton vendeur humain
- Mentionne clairement la baisse de prix
- Cree un sentiment d'urgence SANS etre agressif
"""
    else:
        angle_config = ANGLES_PAR_TYPE.get(vehicle_type, ANGLES_PAR_TYPE["default"])
        exemple = random.choice(angle_config["exemples"])
        focus = angle_config["focus"]
        event_angle = f"""
EVENEMENT: NOUVEAU VEHICULE
- {exemple}
- Focus sur: {focus}
"""

    system_prompt = build_accroche_system_prompt(
        title=title,
        price=price,
        mileage=mileage,
        stock=stock,
        features=features,
        vehicle_type=vehicle_type,
        event_angle=event_angle,
        max_chars=max_chars,
    )

    txt = chat_completion(
        system_prompt=system_prompt,
        user_message=f"Genere une accroche vendeuse et UNIQUE pour ce {title}. Evite absolument les cliches listes.",
        model=get_default_model(),
        max_tokens=150,
        temperature=0.9,
        top_p=0.95,
    )

    if not txt:
        return ""

    txt = filter_cliches(txt)
    return _safe_trim(txt, max_chars)


def humanize_text(raw_text: str, vehicle: Dict[str, Any] = None) -> str:
    """
    Prend un texte genere (souvent robotique) et le rend plus naturel.
    Compatible avec llm.py v2.
    """
    prompt = build_humanize_prompt(raw_text)

    result = chat_completion(
        system_prompt=prompt,
        user_message="Reecris l'introduction.",
        model=get_default_model(),
        max_tokens=300,
        temperature=0.85,
    )

    if not result:
        return raw_text

    result = filter_cliches(result)
    if not result:
        return raw_text

    return result


def generate_intro_only(vehicle: Dict[str, Any], max_chars: int = 250) -> str:
    """
    Genere SEULEMENT une intro accrocheuse.
    Compatible avec llm.py v2.
    """
    try:
        from classifier import classify
    except ImportError:
        def classify(v):
            return "default"

    v = vehicle or {}
    title = (v.get("title") or "Vehicule").strip()
    price = _vehicle_price(v)
    km = _vehicle_mileage(v)
    features = _get_features_text(v)

    vehicle_type = classify(v)
    angle_config = ANGLES_PAR_TYPE.get(vehicle_type, ANGLES_PAR_TYPE["default"])
    focus = angle_config["focus"]

    system_prompt = build_intro_prompt(
        title=title,
        price=price,
        km_formatted=km,
        features=features,
        vehicle_type=vehicle_type,
        focus=focus,
        max_chars=max_chars,
    )

    result = chat_completion(
        system_prompt=system_prompt,
        user_message=f"Genere une intro accrocheuse pour ce {title}.",
        model=get_default_model(),
        max_tokens=120,
        temperature=0.9,
    )

    if not result:
        return ""

    result = filter_cliches(result)
    return _safe_trim(result, max_chars)


# ============================================================
# GENERATION V3 (llm_v3.py compatible)
# ============================================================

def generate_smart_text(
    vehicle: Dict[str, Any],
    event: str = "NEW",
    options_text: str = "",
    old_price=None,
    new_price=None,
) -> Optional[str]:
    """
    Genere un texte Facebook intelligent et humain pour un vehicule.
    Compatible avec llm_v3.py.
    """
    from vehicle_intelligence import build_vehicle_context

    ctx = build_vehicle_context(vehicle)
    if old_price:
        ctx["old_price"] = f"{int(old_price):,}".replace(",", " ") + " $"
    if new_price:
        ctx["new_price"] = f"{int(new_price):,}".replace(",", " ") + " $"

    # Enrichir avec VIN NHTSA
    vin_specs_text = vehicle.get("_vin_specs_text", "")
    if not vin_specs_text:
        try:
            from vin_decoder import decode_vin, format_specs_for_prompt, format_engine_line
            vin_val = (vehicle.get("vin") or "").strip().upper()
            if len(vin_val) >= 11:
                specs = decode_vin(vin_val)
                if specs:
                    vin_specs_text = format_specs_for_prompt(specs)
                    if not ctx.get("hp") and specs.get("engine_hp"):
                        ctx["hp"] = specs["engine_hp"]
                        ctx["engine"] = format_engine_line(specs).replace(f" — {specs['engine_hp']} HP", "")
        except Exception:
            pass

    prompt = build_smart_prompt(ctx, event, options_text)
    if vin_specs_text:
        prompt += f"\n\nCARACTERISTIQUES CERTIFIEES:\n{vin_specs_text}"

    text = chat_completion(
        system_prompt=SYSTEM_PROMPT_V3,
        user_message=prompt,
        model=get_smart_model(),
        max_tokens=1200,
        temperature=0.85,
    )

    if not text:
        return None

    text = remove_cliche_lines(text)
    text = text.strip('"').strip("'")
    return text


def generate_intro_v3(vehicle: Dict[str, Any], max_chars: int = 300) -> Optional[str]:
    """
    Genere SEULEMENT une intro courte et punchy.
    Compatible avec llm_v3.py.
    """
    from vehicle_intelligence import build_vehicle_context
    from pipeline.prompts import INTRO_STYLES

    ctx = build_vehicle_context(vehicle)
    style = random.choice(INTRO_STYLES)

    title = ctx.get("title", "")
    hp = ctx.get("hp", "")
    engine = ctx.get("engine", "")
    trim_vibe = ctx.get("trim_vibe", "")
    model_known_for = ctx.get("model_known_for", "")
    km_desc = ctx.get("km_description", "")
    price_fmt = ctx.get("price_formatted", "")
    vehicle_type = ctx.get("vehicle_type", "general")

    prompt = f"""Ecris SEULEMENT une intro de 2-3 phrases pour cette annonce Facebook.
Vehicule: {title}
Prix: {price_fmt}
KM: {ctx.get('km_formatted', '')} ({km_desc})
{f'Moteur: {engine} — {hp} HP' if hp else ''}
{f'Ce modele: {model_known_for}' if model_known_for else ''}
{f'Ce trim: {trim_vibe}' if trim_vibe else ''}
Style: {style}
Type: {vehicle_type}

REGLES: Max {max_chars} caracteres. Pas d'emojis. Pas de cliches. Pas de "routes de la Beauce".
Parle comme un vrai vendeur quebecois passionne qui connait ses chars.
Mentionne ce qui rend CE vehicule special."""

    text = chat_completion(
        system_prompt=SYSTEM_PROMPT_V3,
        user_message=prompt,
        model=get_smart_model(),
        max_tokens=200,
        temperature=0.9,
    )

    if not text:
        return None

    text = text.strip('"').strip("'")
    return text[:max_chars]
