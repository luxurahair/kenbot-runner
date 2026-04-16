# Backup complet du pipeline de generation - 2026-04-16
# =======================================================
#
# Fichiers sauvegardes:
# - llm.py (v2.0) — Generation AI accroches Facebook
# - llm_v3.py (v3.0) — Generation intelligente par vehicule
# - sticker_to_ad.py — Extraction PDF Window Sticker Stellantis
# - ad_builder.py — Construction annonce structuree
# - classifier.py — Classification type vehicule
# - vehicle_intelligence.py — Base connaissance vehicules
# - footer_utils.py — Gestion footer Daniel Giroux
# - vin_decoder.py — Decodage VIN NHTSA
# - runner_cron_prod.py — Cron principal (orchestrateur)
#
# STRUCTURE ACTUELLE DU PIPELINE:
# ================================
#
# runner_cron_prod.py (orchestrateur)
#   |
#   |-- PRIORITE 1: Stellantis + Sticker PDF + AI
#   |     sticker_to_ad.py → extraction options PDF
#   |     llm_v3.py → generate_smart_text (humanisation)
#   |     vehicle_intelligence.py → contexte vehicule
#   |
#   |-- PRIORITE 2: LLM_V3 (tous vehicules, avec VIN)
#   |     llm_v3.py → generate_smart_text
#   |     vin_decoder.py → specs NHTSA
#   |     vehicle_intelligence.py → contexte
#   |
#   |-- PRIORITE 3: Sticker brut (sans AI)
#   |     sticker_to_ad.py → build_ad
#   |     llm.py → generate_intro_only (intro seulement)
#   |
#   |-- PRIORITE 4: Text engine externe
#   |     text_engine_client.py
#   |     llm.py → generate_ad_text (accroche)
#   |
#   |-- FOOTER:
#         footer_utils.py → add_footer_if_missing
#
# IMPORTS DANS runner_cron_prod.py:
#   from sticker_to_ad import extract_spans_pdfminer, extract_option_groups_from_spans
#   from ad_builder import build_ad as build_ad_from_options
#   from llm import generate_ad_text, humanize_text, generate_intro_only
#   from llm_v3 import generate_smart_text as generate_smart_text_v3
#   from vehicle_intelligence import build_vehicle_context
#   from footer_utils import add_footer_if_missing, has_footer, get_dealer_footer
#   from vin_decoder import decode_vin, format_specs_for_prompt
#   from classifier import classify
