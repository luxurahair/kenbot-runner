"""
llm_v3.py

Génération de textes Facebook HUMAINS et INTELLIGENTS pour les annonces auto.
Utilise vehicle_intelligence.py pour adapter le ton, les angles et le contenu
à chaque véhicule spécifique.

Version 3.0 — Textes qui sonnent comme un vrai vendeur passionné, pas un robot.
"""

import os
import random
from typing import Dict, Any, Optional, List

from vehicle_intelligence import build_vehicle_context, humanize_options


# ─── OpenAI client ───
def _get_openai():
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            return None
        return OpenAI(api_key=api_key)
    except ImportError:
        return None


# ─── Prompts par type de véhicule ───

SYSTEM_PROMPT = """Tu es un vendeur chez Kennebec Dodge Chrysler a Saint-Georges.
Tu ecris des annonces Facebook pour des vehicules automobiles (neufs OU occasions).

REGLES DE CONDITION (TRES IMPORTANT):
- Le contexte te fournit CONDITION: "neuf" ou "occasion".
- SI CONDITION = "neuf":
  * NE JAMAIS utiliser: "occasion", "usage", "usagé", "comme neuf", "inspection", "historique", "faible kilometrage", "un seul proprietaire", "ancien proprietaire", "reprise"
  * Mettre l'accent sur: disponibilite, equipements confirmes, version/trim, garantie constructeur, financement promotionnel (seulement si confirme)
  * Le kilometrage peut etre mentionne comme "0 km" ou "flambant neuf" mais n'en faire pas un argument de vente
- SI CONDITION = "occasion":
  * Le kilometrage PEUT etre un argument (ex: "faible kilometrage", "28 000 km seulement")
  * Tu peux valoriser l'etat, l'entretien, l'historique verifie
  * Eviter "flambant neuf" ou "direct de l'usine"

REGLES ABSOLUES:
- Tu ecris en francais quebecois naturel. Pas de francais de France. Pas de robot.
- Tu parles comme un VRAI vendeur qui connait ses chars. Pas de phrases generiques.
- JAMAIS mentionner "la Beauce", "routes de la Beauce" ou "paysages beauceron". On vend des chars, pas du tourisme.
- ABSOLUMENT AUCUN mot vulgaire, grossier ou a caractere sexuel.
- Chaque texte doit etre UNIQUE.
- Le ton est direct, authentique, passionne.

REGLES INTRO — CRITIQUES:
- JAMAIS commencer par parler de toi, de ton experience, de ta passion ou de tes annees dans le metier.
- JAMAIS "En tant qu'expert automobile", "Passionne depuis...", "Apres deux decennies...", "Avec mes X annees d'experience..."
- JAMAIS "faire tourner les tetes", "experience de conduite exceptionnelle", "choix exceptionnel", "veritable partenaire", "pret a dominer", "conquérir les chemins", "sillonner", "ce bijou", "cette merveille"
- TOUJOURS commencer par LE VEHICULE. La premiere phrase doit parler du CHAR, pas du vendeur.
- Varie les ouvertures: question au lecteur, chiffre (HP, km), mise en situation, fait precis, prix, rarete.
- Maximum 2-3 phrases pour l'intro. Court et punchy.
- Pas de hashtags dans l'intro.
- Pas d'emojis dans l'intro (ils viennent apres dans le corps de l'annonce).
- JAMAIS mentionner "Daniel Giroux" ou tout nom de vendeur dans l'intro.
- Le PRIX doit TOUJOURS apparaitre clairement dans le corps (ex: "💰 34 995 $").
"""

def _build_prompt_for_vehicle(ctx: Dict[str, Any], event: str = "NEW", options_text: str = "") -> str:
    """Construit le prompt spécifique au véhicule."""

    # Info de base
    title = ctx.get("title", "")
    brand = ctx.get("brand", "").capitalize()
    model = ctx.get("model", "")
    trim = ctx.get("trim", "")
    year = ctx.get("year", "")
    price_fmt = ctx.get("price_formatted", "")
    km_fmt = ctx.get("km_formatted", "")
    km_desc = ctx.get("km_description", "")
    price_desc = ctx.get("price_description", "")

    # Intelligence véhicule
    vehicle_type = ctx.get("vehicle_type", "general")
    condition = (ctx.get("condition") or "occasion").strip().lower()
    hp = ctx.get("hp", "")
    engine = ctx.get("engine", "")
    trim_vibe = ctx.get("trim_vibe", "")
    model_known_for = ctx.get("model_known_for", "")
    brand_identity = ctx.get("brand_identity", "")
    brand_angles = ctx.get("brand_angles", [])

    # Construire les infos spécifiques
    specs_info = []
    if hp:
        specs_info.append(f"Moteur: {engine} — {hp} chevaux")
    elif engine:
        specs_info.append(f"Moteur: {engine}")
    if trim_vibe:
        specs_info.append(f"Ce trim: {trim_vibe}")
    if model_known_for:
        specs_info.append(f"Ce modèle est connu pour: {model_known_for}")
    if brand_identity:
        specs_info.append(f"La marque {brand}: {brand_identity}")

    # Options humanisées
    human_options = []
    if options_text:
        human_options = humanize_options(options_text)

    prompt = f"""Écris une annonce Facebook pour ce véhicule:

VÉHICULE: {title}
CONDITION: {condition}  ← IMPORTANT: adapte ton discours (neuf=pas de km/historique, occasion=km/etat OK)
PRIX: {price_fmt}
KILOMÉTRAGE: {km_fmt} ({km_desc})
POSITIONNEMENT PRIX: {price_desc}
TYPE: {vehicle_type}

CONNAISSANCES SPÉCIFIQUES:
{chr(10).join(specs_info) if specs_info else "Aucune info spécifique disponible."}

OPTIONS/ÉQUIPEMENTS CONFIRMÉS:
{chr(10).join(f"- {o}" for o in human_options) if human_options else "Aucune option confirmée."}

ANGLES DE VENTE SUGGÉRÉS: {', '.join(brand_angles[:3]) if brand_angles else 'qualité, valeur, confiance'}

INSTRUCTIONS:
1. Ecris une INTRO de 2-3 phrases maximum. COURTE et PUNCHY.
   REGLES CRITIQUES POUR L'INTRO:
   - La PREMIERE phrase doit parler DU VEHICULE, jamais du vendeur
   - INTERDIT de commencer par ton experience, ta passion, tes annees dans le metier
   - INTERDIT: "En tant qu'expert", "Passionne depuis...", "Apres deux decennies..."
   - INTERDIT: "faire tourner les tetes", "choix exceptionnel", "veritable partenaire"
   - Commence par le CHAR: son nom, ses specs, son prix, sa rarete, un chiffre
   - Adapte le ton au type: {"adrenaline et son du moteur" if vehicle_type == "muscle_car" else "robustesse et capacite" if vehicle_type in ("pickup", "pickup_hd") else "aventure et liberte" if vehicle_type == "off_road" else "confort et raffinement" if vehicle_type == "suv_premium" else "style et economie" if vehicle_type in ("citadine", "suv_compact") else "exclusivite et reve" if vehicle_type in ("exotique", "collector") else "polyvalence et fiabilite"}

2. Puis le CORPS structuré:
   - Titre avec le nom complet et l'année
   - PRIX EN GROS (OBLIGATOIRE — le prix doit TOUJOURS apparaître clairement, ex: "💰 34 995 $")
   - Kilométrage
   - Stock
   - 5-8 équipements/caractéristiques en points (en français, pas de jargon technique brut)
   - Si c'est un Stellantis avec sticker: mention "Window Sticker vérifié"

3. NE METS PAS de nom de vendeur dans le texte (ni "Daniel Giroux" ni aucun nom). NE METS PAS de footer, de coordonnées, de hashtags. Le footer sera ajouté automatiquement après.
   NE METS PAS 'NHTSA', 'VIN decode', 'PROFIL DU VÉHICULE', 'Type: pickup_hd' ou tout terme/etiquette technique interne.
   Les specs certifiees peuvent etre integrees dans le corps, mais reformulees naturellement (pas recopiees brutes).

FORMAT DE SORTIE: Texte prêt à copier-coller sur Facebook. Utilise des emojis avec parcimonie dans le corps (pas dans l'intro).
"""

    if event == "PRICE_CHANGED":
        old_price = ctx.get("old_price", "")
        new_price = ctx.get("new_price", "")
        prompt += f"""
ÉVÉNEMENT SPÉCIAL: BAISSE DE PRIX
Ancien prix: {old_price}
Nouveau prix: {new_price}
→ Commence par mentionner la baisse de prix de façon naturelle et excitante.
→ Fais sentir que c'est une opportunité sans être agressif.
"""

    return prompt


# ─── Variations d'intro pour éviter la répétition ───
INTRO_STYLES = [
    "NOUVELLE ARRIVAGE: Commence par 'Nouvelle arrivage!' ou 'Juste rentre!' puis decris le char",
    "CHIFFRE PUNCH: Commence par un chiffre (HP, km bas, prix). Ex: '395 chevaux, 25 000 km.'",
    "QUESTION CLIENT: Pose une question directe. Ex: 'Tu cherches un pickup fiable?'",
    "LE CHAR PARLE: Decris le vehicule. Ex: 'Regarde-moi ce RAM la...'",
    "OCCASION RARE: Mets l'accent sur la rarete. Ex: 'Un Rubicon 2024 a ce prix, ca se voit pas souvent.'",
    "PRIX/DEAL: Commence par le prix. Ex: 'A 34 995$, t'auras pas mieux.'",
    "POUR QUI: Commence par le client ideal. Ex: 'Pour celui qui a besoin d'un vrai camion...'",
    "SPEC TECHNIQUE: La spec qui tue. Ex: 'V8 6.4L, 485 chevaux. Point final.'",
    "HISTOIRE COURTE: Mini contexte. Ex: 'Un seul proprio, entretien chez nous.'",
    "SAISON: Lie au moment. Ex: 'Juste a temps pour l'ete!' ou 'Un 4x4 ca se refuse pas.'",
]


def generate_smart_text(
    vehicle: Dict[str, Any],
    event: str = "NEW",
    options_text: str = "",
    old_price: Any = None,
    new_price: Any = None,
) -> Optional[str]:
    """
    Génère un texte Facebook intelligent et humain pour un véhicule.

    Args:
        vehicle: Dict avec title, stock, vin, price_int, km_int, url, etc.
        event: "NEW", "PRICE_CHANGED", "PHOTOS_ADDED"
        options_text: Texte brut des options du sticker (optionnel)
        old_price: Ancien prix (pour PRICE_CHANGED)
        new_price: Nouveau prix (pour PRICE_CHANGED)

    Returns:
        Texte Facebook prêt à publier, ou None si échec
    """
    client = _get_openai()
    if not client:
        return None

    # Construire le contexte enrichi
    ctx = build_vehicle_context(vehicle)
    if old_price:
        ctx["old_price"] = f"{int(old_price):,}".replace(",", " ") + " $"
    if new_price:
        ctx["new_price"] = f"{int(new_price):,}".replace(",", " ") + " $"

    # Enrichir avec les specs VIN NHTSA si disponibles
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

    # Choisir un style d'intro aléatoire
    style = random.choice(INTRO_STYLES)

    # Construire le prompt
    prompt = _build_prompt_for_vehicle(ctx, event, options_text)
    if vin_specs_text:
        prompt += f"\n\n[SPECS CERTIFIEES — a integrer dans le corps, reformulees]\n{vin_specs_text}"
    prompt += f"\n\nSTYLE D'INTRO: {style}"

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.85,
            max_tokens=1200,
        )

        text = response.choices[0].message.content.strip()

        # Post-traitement
        text = _post_process(text)
        return text

    except Exception as e:
        print(f"[LLM_V3 ERROR] {e}", flush=True)
        return None


def generate_intro_v3(vehicle: Dict[str, Any], max_chars: int = 300) -> Optional[str]:
    """
    Génère SEULEMENT une intro courte et punchy pour un véhicule.
    Utilisée pour ajouter au-dessus d'un texte existant.
    """
    client = _get_openai()
    if not client:
        return None

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

    prompt = f"""Écris SEULEMENT une intro de 2-3 phrases pour cette annonce Facebook.
Véhicule: {title}
Prix: {price_fmt}
KM: {ctx.get('km_formatted', '')} ({km_desc})
{f'Moteur: {engine} — {hp} HP' if hp else ''}
{f'Ce modèle: {model_known_for}' if model_known_for else ''}
{f'Ce trim: {trim_vibe}' if trim_vibe else ''}
Style: {style}
Type: {vehicle_type}

RÈGLES: Max {max_chars} caractères. Pas d'emojis. Pas de clichés. Pas de "routes de la Beauce". 
Parle comme un vrai vendeur québécois passionné qui connaît ses chars.
Mentionne ce qui rend CE véhicule spécial."""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.9,
            max_tokens=200,
        )
        text = response.choices[0].message.content.strip()
        text = text.strip('"').strip("'")
        return text[:max_chars]
    except Exception as e:
        print(f"[LLM_V3 INTRO ERROR] {e}", flush=True)
        return None


def _post_process(text: str) -> str:
    """Nettoyage post-génération. Utilise le filtre centralisé pipeline/cliches.py."""
    # Retirer les guillemets englobants
    text = text.strip('"').strip("'")

    # Utiliser le filtre centralisé (source unique de vérité)
    try:
        from pipeline.cliches import remove_cliche_lines
        text = remove_cliche_lines(text)
        return text.strip()
    except Exception as e:
        print(f"[LLM_V3 POSTPROCESS] pipeline.cliches indisponible: {e}", flush=True)

    # Fallback: liste locale minimale
    cliches = [
        "prêt à dominer", "pret a dominer",
        "faire tourner les têtes", "faire tourner les tetes",
        "sillonner la beauce", "sillonner les routes",
        "conquérir les chemins", "conquerir les chemins",
        "dominer les routes",
        "parcourir les routes de beauce",
        "arpenter les routes",
        "routes de la beauce", "routes de beauce",
        "chemins de la beauce",
        "paysages de la beauce", "paysages beauceron",
        "en tant qu'expert", "en tant qu expert", "expert automobile",
        "20 ans d'expérience", "20 ans d experience",
        "près de 20 ans", "pres de 20 ans",
        "années d'expérience", "annees d'experience",
        "passionné par les voitures depuis", "passionne par les voitures depuis",
        "après deux décennies", "apres deux decennies",
        "deux décennies d'expérience", "deux decennies d experience",
        "en tant que passionné", "en tant que passionne",
        "expérience de conduite exceptionnelle",
        "cette merveille", "ce bijou", "cette beauté",
        "véritable partenaire", "veritable partenaire",
        "choix exceptionnel", "n'attend plus que toi",
        "parfait pour l'hiver", "vous séduira",
        "esprit d'aventure", "sensations fortes",
        "fiabilité légendaire", "fiabilite legendaire",
        "ravi de vous présenter", "ravi de vous presenter",
        "permettez-moi", "permettez moi",
        "ne manquera pas d'impressionner",
        "saura combler", "saura impressionner",
    ]
    for c in cliches:
        if c in text.lower():
            lines = text.split("\n")
            text = "\n".join(l for l in lines if c not in l.lower())

    # Retirer les mots vulgaires/sexuels
    vulgar = ["couilles", "balls", "badass", "bitch", "cul ", "merde", "crisse",
              "tabarnac", "calisse", "ostie", "fuck", "shit", "damn", "ass ", "sexy"]
    for v in vulgar:
        if v in text.lower():
            lines = text.split("\n")
            text = "\n".join(l for l in lines if v not in l.lower())

    return text.strip()


# ─── Test local ───
if __name__ == "__main__":
    # Test avec quelques véhicules
    test_vehicles = [
        {"title": "Dodge CHALLENGER R/T SCAT PACK BLANC 2023", "stock": "06234", "vin": "2C3CDZFJ1PH593481", "price_int": 79995, "km_int": 11500},
        {"title": "Jeep WRANGLER RUBICON 4XE 2024", "stock": "06106", "vin": "1C4HJXFN5RW123456", "price_int": 62995, "km_int": 15586},
        {"title": "Ram 2500 BIG HORN 2025", "stock": "06230", "vin": "3C6UR5DJ1RG123456", "price_int": 71995, "km_int": 25},
        {"title": "LAMBORGHIN I 2024", "stock": "06232", "vin": "", "price_int": 343995, "km_int": 8900},
        {"title": "Ford MUSTANG 2022", "stock": "46104A", "vin": "", "price_int": 35995, "km_int": 21433},
        {"title": "Fiat 500 E RED 2024", "stock": "44220A", "vin": "", "price_int": 23995, "km_int": 22},
    ]

    for v in test_vehicles:
        print(f"\n{'='*60}")
        print(f"TEST: {v['title']}")
        print(f"{'='*60}")
        ctx = build_vehicle_context(v)
        print(f"  Brand: {ctx['brand']} | Model: {ctx['model']} | Trim: {ctx['trim']}")
        print(f"  Type: {ctx['vehicle_type']} | HP: {ctx['hp']} | Engine: {ctx['engine']}")
        print(f"  Vibe: {ctx['trim_vibe']}")
        print(f"  KM: {ctx['km_description']} | Prix: {ctx['price_description']}")
        print()

        text = generate_smart_text(v)
        if text:
            print(text[:500])
        else:
            print("  [Pas de clé OpenAI — test parsing seulement]")
