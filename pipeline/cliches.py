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
