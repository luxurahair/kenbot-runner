"""
pipeline/cliches.py — Filtre anti-cliches centralise.
Source unique de verite pour les cliches interdits et le filtrage.
"""

import re
from typing import List

# ============================================================
# CLICHES INTERDITS — Liste maitre
# ============================================================

CLICHES_INTERDITS_LIST: List[str] = [
    "sillonner les routes",
    "sillonner la beauce",
    "parfait pour l'hiver",
    "parfait pour l'hiver québécois",
    "affronter l'hiver",
    "prêt à conquérir",
    "prête à dominer",
    "dominer les routes",
    "conçu pour affronter",
    "idéal pour les aventures",
    "n'attend plus que toi",
    "ce bijou",
    "cette merveille",
    "cette beauté",
    "viens le voir",
    "véritable machine",
    "monstre de puissance",
    "bête de route",
    "sensation de conduite",
    "vous séduira",
    "ne cherchez plus",
    "l'occasion parfaite",
    "faire tourner les têtes",
    "conquérir les chemins",
    "parcourir les routes de beauce",
    "arpenter les routes",
    "routes de la beauce",
    "routes de beauce",
    "chemins de la beauce",
    "paysages de la beauce",
    "paysages beauceron",
    "faire tourner les têtes",
    "faire tourner les tetes",
    "passionné par les voitures depuis",
    "passionne par les voitures depuis",
    "en tant que passionné",
    "en tant que passionne",
    "deux décennies d'expérience",
    "deux decennies d'experience",
    "expérience de conduite exceptionnelle",
    "experience de conduite exceptionnelle",
    "saura répondre à vos besoins",
    "saura repondre a vos besoins",
    "ne manque pas de",
    "véritable partenaire",
    "veritable partenaire",
    "choix exceptionnel",
    # === Expertise / experience (bannis — ne JAMAIS parler du vendeur dans l'intro) ===
    # NOTE: "près de 20 ans" seul est exclu car c'est dans le footer Daniel Giroux
    # (signature legitime "Conseiller expert depuis près de 20 ans"). On bloque
    # seulement les formulations claires d'intro qui citent l'expertise.
    "en tant qu'expert",
    "en tant qu expert",
    "expert automobile",
    "comme expert",
    "mon expertise",
    "mon avis personnel",
    "de mon avis",
    "à mon avis",
    "a mon avis",
    "20 ans d'expérience",
    "20 ans d experience",
    "vingt ans d'expérience",
    "vingt ans d experience",
    "années d'expérience",
    "annees d'experience",
    "après deux décennies",
    "apres deux decennies",
    "avec mes années",
    "avec mes annees",
    "ravi de vous présenter",
    "ravi de vous presenter",
    "je suis ravi",
    "permettez-moi de vous",
    "permettez moi de vous",
    "permettez-moi de vous partager",
    "partager mon enthousiasme",
    "mon enthousiasme",
    # === Clichés marketing génériques (vu dans posts recents) ===
    "esprit d'aventure",
    "esprit d aventure",
    "esprit de liberté",
    "esprit de liberte",
    "sortir des sentiers battus",
    "sensations fortes",
    "amateurs de sensations",
    "fiabilité légendaire",
    "fiabilite legendaire",
    "capacités exceptionnelles",
    "capacites exceptionnelles",
    "capacités hors-route exceptionnelles",
    "capacites hors route exceptionnelles",
    "ne manquera pas d'impressionner",
    "ne manquera pas d impressionner",
    "saura combler",
    "saura impressionner",
    "incarne l'esprit",
    "incarne l esprit",
    "confort surprenant",
    "confort incomparable",
    "expérience inoubliable",
    "experience inoubliable",
]

# Version texte pour injection dans les prompts
CLICHES_INTERDITS_PROMPT = """
PHRASES STRICTEMENT INTERDITES (ne JAMAIS utiliser):
""" + "\n".join(f"- \"{c}\"" for c in CLICHES_INTERDITS_LIST)

# Mots vulgaires interdits
VULGAR_WORDS: List[str] = [
    "couilles", "balls", "badass", "bitch", "cul ", "merde", "crisse",
    "tabarnac", "calisse", "ostie", "fuck", "shit", "damn", "ass ", "sexy",
]


def filter_cliches(text: str) -> str:
    """
    Filtre de securite: retire le texte si un cliche est detecte.
    Retourne le texte original ou chaine vide si cliche trouve.
    """
    if not text:
        return ""
    low = text.lower()
    for cliche in CLICHES_INTERDITS_LIST:
        if cliche in low:
            print(f"[PIPELINE CLICHE] Detecte et filtre: {cliche}", flush=True)
            return ""
    return text


def remove_cliche_lines(text: str) -> str:
    """
    Retire les LIGNES contenant un cliche, sans vider tout le texte.
    Utilise par llm_v3 post-processing.
    """
    if not text:
        return ""
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        low = line.lower()
        has_cliche = any(c in low for c in CLICHES_INTERDITS_LIST)
        has_vulgar = any(v in low for v in VULGAR_WORDS)
        if not has_cliche and not has_vulgar:
            cleaned.append(line)
    return "\n".join(cleaned).strip()



# ============================================================
# SANITIZER ULTIME — dernière ligne de défense avant publication FB
# ============================================================

# Types internes (vehicle_type) qui ne doivent JAMAIS apparaître dans le texte
_INTERNAL_TYPES = (
    "pickup_hd", "pickup", "muscle_car", "off_road", "suv_premium",
    "suv_compact", "citadine", "exotique", "collector", "general",
    "sedan", "coupe", "sport", "van", "minivan",
)

# Patterns de fuite de contexte ("internal brand identity" -> ne doit pas fuir)
_LEAKED_MARQUE_PATTERNS = (
    r"le truck qui travaille", r"le char qui", r"la bête", r"la bete",
    r"le heavy-duty qui", r"le vus qui", r"le pickup qui",
    r"la citadine qui", r"la sportive qui", r"le suv qui",
)

_LEAKED_MODELE_PATTERNS = (
    r"le heavy-duty qui remorque", r"le truck qui", r"le char qui",
    r"la b[êe]te qui", r"le vus qui", r"la citadine qui", r"le pickup qui",
)


def sanitize_ad_text(text: str) -> str:
    """
    Dernière défense avant publication Facebook.
    Retire TOUTES les fuites de contexte interne et liens morts.
    Appelée sur chaque base_text juste avant FB.create_post / FB.update_post.
    """
    if not text:
        return ""

    # 1) Remplacer les vieux tinyurl par le vrai lien direct
    text = text.replace(
        "tinyurl.com/EvaluerMonAuto",
        "kenbot-dashboard-five.vercel.app/reprise",
    )
    text = text.replace(
        "https://tinyurl.com/EvaluerMonAuto",
        "https://kenbot-dashboard-five.vercel.app/reprise",
    )

    # 2) Supprimer le bloc complet "PROFIL DU VÉHICULE" (fuite de prompt)
    #    Ex: "PROFIL DU VÉHICULE:\nMarque: le truck...\nModèle: ...\nType: pickup_hd\n"
    text = re.sub(
        r"\n*\s*PROFIL\s+DU\s+V[ÉE]HICULE\s*:\s*\n"
        r"(?:[ \t]*(?:Marque|Mod[èe]le|Type|Brand|Model)\s*:[^\n]*\n?)+",
        "\n",
        text,
        flags=re.IGNORECASE,
    )

    # 3) Retirer les lignes "Type: <type_interne>" orphelines
    types_pat = "|".join(_INTERNAL_TYPES)
    text = re.sub(
        rf"^[ \t]*Type\s*:\s*({types_pat})\s*\n?",
        "",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # 4) Retirer les lignes "Marque: <identity_interne>" orphelines
    for pat in _LEAKED_MARQUE_PATTERNS:
        text = re.sub(
            rf"^[ \t]*Marque\s*:\s*[^\n]*{pat}[^\n]*\n?",
            "",
            text,
            flags=re.IGNORECASE | re.MULTILINE,
        )

    # 5) Retirer les lignes "Modèle: <known_for_interne>" orphelines
    for pat in _LEAKED_MODELE_PATTERNS:
        text = re.sub(
            rf"^[ \t]*Mod[èe]le\s*:\s*[^\n]*{pat}[^\n]*\n?",
            "",
            text,
            flags=re.IGNORECASE | re.MULTILINE,
        )

    # 6) Si une ligne "Marque:" ou "Modèle:" est suivie uniquement de texte de
    #    tone-tag (identity brute, pas une vraie info), la retirer.
    #    On ne retire QUE quand la valeur commence par "le " ou "la " et est
    #    un fragment de tone-tag identifiable — pas les vraies infos "Marque: Ram 2500"
    text = re.sub(
        r"^[ \t]*Marque\s*:\s*(?:le|la)\s+(?:truck|char|vus|pickup|suv|b[êe]te|citadine|sportive)[^\n]*\n?",
        "",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    text = re.sub(
        r"^[ \t]*Mod[èe]le\s*:\s*(?:le|la)\s+(?:heavy-?duty|truck|char|vus|pickup|suv|b[êe]te|citadine|sportive)[^\n]*\n?",
        "",
        text,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # 7) Normaliser les retours à la ligne multiples créés par les suppressions
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)

    return text.strip()


def has_leak(text: str) -> bool:
    """Détecte s'il reste une fuite de contexte après/avant sanitize."""
    if not text:
        return False
    low = text.lower()
    if "tinyurl.com/evaluermonauto" in low:
        return True
    if re.search(r"profil\s+du\s+v[ée]hicule\s*:", low):
        return True
    for t in _INTERNAL_TYPES:
        if re.search(rf"\btype\s*:\s*{re.escape(t)}\b", low):
            return True
    for pat in _LEAKED_MARQUE_PATTERNS + _LEAKED_MODELE_PATTERNS:
        if re.search(pat, low):
            return True
    return False
