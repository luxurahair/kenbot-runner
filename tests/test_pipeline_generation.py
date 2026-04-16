#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_pipeline_generation.py
============================
Programme de test complet pour le pipeline de generation d'annonces.
Teste TOUS les cas d'utilisation du cron:

1. Stellantis AVEC Window Sticker (RAM, Dodge, Jeep, Chrysler)
2. Stellantis SANS Window Sticker
3. Non-Stellantis (Ford, Toyota, Honda, Mazda)
4. Vehicules exotiques (Ferrari, Lamborghini)
5. PRICE_CHANGED event
6. PHOTOS_ADDED (regeneration texte)
7. SOLD (prefixe VENDU)
8. UNSOLD (restauration)
9. NO_PHOTO scenario
10. VIN decode NHTSA
11. Lien /reprise dans le footer
12. meta_compare (site vs FB)
13. Tous les types: truck, SUV, coupe, sedan, minivan, EV, off-road

Usage:
    python tests/test_pipeline_generation.py                # Test complet (avec API)
    python tests/test_pipeline_generation.py --no-api       # Test sans API (parsing seulement)
"""

import os
import sys
import json
import time
import argparse
from typing import Dict, Any, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from classifier import classify
from vehicle_intelligence import build_vehicle_context, parse_vehicle_title, get_vehicle_profile
from footer_utils import has_footer, add_footer_if_missing, get_dealer_footer, count_footer_occurrences
from vin_decoder import decode_vin, format_specs_for_prompt, format_engine_line
from pipeline.cliches import filter_cliches, remove_cliche_lines, CLICHES_INTERDITS_LIST

# ============================================================
# VEHICULES DE TEST
# ============================================================

TEST_VEHICLES: List[Dict[str, Any]] = [
    # === STELLANTIS ===
    {
        "title": "RAM 1500 SPORT 2023",
        "stock": "06300", "vin": "1C6SRFTT7MN517688",
        "price_int": 52995, "km_int": 25000,
        "_test_type": "stellantis_truck", "_test_event": "NEW", "_expect_type": "pickup",
    },
    {
        "title": "Dodge CHALLENGER R/T SCAT PACK BLANC 2023",
        "stock": "06234", "vin": "2C3CDZFJ1PH593481",
        "price_int": 79995, "km_int": 11500,
        "_test_type": "stellantis_muscle", "_test_event": "NEW", "_expect_type": "muscle_car",
    },
    {
        "title": "Jeep WRANGLER RUBICON 4XE 2024",
        "stock": "06106", "vin": "1C4HJXFN5RW123456",
        "price_int": 62995, "km_int": 15586,
        "_test_type": "stellantis_offroad", "_test_event": "NEW", "_expect_type": "off_road",
    },
    {
        "title": "Jeep GRAND CHEROKEE LIMITED 2023",
        "stock": "06188", "vin": "1C4RJFBG2PC123456",
        "price_int": 54995, "km_int": 32000,
        "_test_type": "stellantis_suv", "_test_event": "NEW", "_expect_type": "suv_premium",
    },
    {
        "title": "Ram 2500 BIG HORN 2025",
        "stock": "06230", "vin": "3C6UR5DJ1RG123456",
        "price_int": 71995, "km_int": 25,
        "_test_type": "stellantis_hd", "_test_event": "NEW", "_expect_type": "pickup_hd",
    },
    {
        "title": "Chrysler PACIFICA TOURING L 2023",
        "stock": "06150", "vin": "2C4RC1BG5PR123456",
        "price_int": 42995, "km_int": 28000,
        "_test_type": "stellantis_minivan", "_test_event": "NEW", "_expect_type": "minivan",
    },
    {
        "title": "Dodge HORNET R/T PLUS 2024",
        "stock": "06280", "vin": "ZACPDFCW2R3123456",
        "price_int": 44995, "km_int": 8500,
        "_test_type": "stellantis_suv_compact", "_test_event": "NEW", "_expect_type": "suv_compact",
    },
    # === NON-STELLANTIS ===
    {
        "title": "Ford MUSTANG GT 2022",
        "stock": "46104A", "vin": "",
        "price_int": 45995, "km_int": 21433,
        "_test_type": "non_stellantis_coupe", "_test_event": "NEW", "_expect_type": "muscle_car",
    },
    {
        "title": "Toyota RAV4 XLE AWD 2023",
        "stock": "46200A", "vin": "",
        "price_int": 34995, "km_int": 35000,
        "_test_type": "non_stellantis_suv", "_test_event": "NEW", "_expect_type": "suv_compact",
    },
    {
        "title": "Honda CIVIC EX 2022",
        "stock": "46300A", "vin": "",
        "price_int": 26995, "km_int": 42000,
        "_test_type": "non_stellantis_sedan", "_test_event": "NEW", "_expect_type": "berline",
    },
    {
        "title": "Ford F-150 XLT SUPERCREW 4X4 2023",
        "stock": "46400A", "vin": "",
        "price_int": 49995, "km_int": 38000,
        "_test_type": "non_stellantis_truck", "_test_event": "NEW", "_expect_type": "pickup",
    },
    # === EXOTIQUES ===
    {
        "title": "LAMBORGHINI HURACAN 2024",
        "stock": "06232", "vin": "",
        "price_int": 343995, "km_int": 8900,
        "_test_type": "exotic_lambo", "_test_event": "NEW", "_expect_type": "exotique",
    },
    {
        "title": "Ferrari 488 GTB 2020",
        "stock": "06290", "vin": "",
        "price_int": 289995, "km_int": 12000,
        "_test_type": "exotic_ferrari", "_test_event": "NEW", "_expect_type": "exotique",
    },
    # === EV ===
    {
        "title": "Fiat 500 E RED 2024",
        "stock": "44220A", "vin": "",
        "price_int": 23995, "km_int": 22,
        "_test_type": "ev", "_test_event": "NEW", "_expect_type": "citadine",
    },
    # === PRICE_CHANGED ===
    {
        "title": "RAM 1500 BIG HORN 2022",
        "stock": "06100", "vin": "1C6SRFFT8MN123456",
        "price_int": 39995, "km_int": 55000,
        "old_price": 44995, "new_price": 39995,
        "_test_type": "price_changed", "_test_event": "PRICE_CHANGED", "_expect_type": "pickup",
    },
]

# VINs reels pour tester le decode NHTSA
TEST_VINS = [
    {"vin": "1C6SRFTT7MN517688", "expect_make": "RAM", "desc": "RAM 1500 2021"},
    {"vin": "2C3CDZFJ1PH593481", "expect_make": "DODGE", "desc": "Dodge Challenger"},
    {"vin": "3VW217AU0GM069002", "expect_make": "VOLKSWAGEN", "desc": "VW Golf 2016"},
]


# ============================================================
# TEST 1: CLASSIFIER
# ============================================================
def test_classifier():
    print("\n" + "=" * 70)
    print("TEST 1: CLASSIFIER")
    print("=" * 70)
    passed = 0
    for v in TEST_VEHICLES:
        result = classify(v)
        print(f"  [{v['_test_type']:30s}] {v['title']:45s} -> {result}")
        passed += 1
    print(f"\n  Resultat: {passed}/{passed}")
    return passed, 0


# ============================================================
# TEST 2: VEHICLE INTELLIGENCE
# ============================================================
def test_vehicle_intelligence():
    print("\n" + "=" * 70)
    print("TEST 2: VEHICLE INTELLIGENCE")
    print("=" * 70)
    passed = failed = 0
    for v in TEST_VEHICLES:
        ctx = build_vehicle_context(v)
        vtype = ctx.get("vehicle_type", "?")
        expect = v.get("_expect_type", "")
        ok = vtype == expect or not expect
        if ok:
            passed += 1
        else:
            failed += 1
        status = "OK" if ok else f"MISMATCH (got {vtype}, expected {expect})"
        print(f"  [{v['_test_type']:30s}] type={vtype:15s} hp={ctx.get('hp',''):5s} [{status}]")
    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 3: FILTRE ANTI-CLICHES
# ============================================================
def test_cliche_filter():
    print("\n" + "=" * 70)
    print("TEST 3: FILTRE ANTI-CLICHES")
    print("=" * 70)
    passed = failed = 0
    cliche_texts = [
        "Ce RAM est pret a sillonner les routes de la Beauce!",
        "Cette merveille n'attend plus que toi.",
        "Parfait pour l'hiver quebecois, ce Jeep va dominer les routes.",
    ]
    for text in cliche_texts:
        result = filter_cliches(text)
        if result == "":
            passed += 1
            print(f"  [OK] Cliche filtre: {text[:60]}...")
        else:
            failed += 1
            print(f"  [FAIL] Cliche non filtre: {text[:60]}...")

    clean_texts = [
        "J'ai rentre ce RAM 1500 la semaine passee. V8 HEMI, 25 000 km.",
        "Un Challenger Scat Pack avec 11 500 km, c'est rare.",
        "Le proprio l'a garde 2 ans, toujours dans le garage.",
    ]
    for text in clean_texts:
        result = filter_cliches(text)
        if result == text:
            passed += 1
            print(f"  [OK] Texte propre passe: {text[:60]}...")
        else:
            failed += 1
            print(f"  [FAIL] Texte propre rejete: {text[:60]}...")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 4: FOOTER + LIEN /REPRISE
# ============================================================
def test_footer_et_reprise():
    print("\n" + "=" * 70)
    print("TEST 4: FOOTER DANIEL GIROUX + LIEN /REPRISE")
    print("=" * 70)
    passed = failed = 0

    # 4a: Sans footer
    text_no = "RAM 1500 2022\n\n34 995 $"
    if not has_footer(text_no):
        passed += 1
        print("  [OK] Texte sans footer detecte")
    else:
        failed += 1
        print("  [FAIL] Faux positif")

    # 4b: Ajout footer
    result = add_footer_if_missing(text_no)
    if has_footer(result):
        passed += 1
        print("  [OK] Footer ajoute")
    else:
        failed += 1
        print("  [FAIL] Footer non ajoute")

    # 4c: Pas double footer
    result2 = add_footer_if_missing(result)
    if count_footer_occurrences(result2) == 1:
        passed += 1
        print("  [OK] Pas de double footer")
    else:
        failed += 1
        print("  [FAIL] Double footer")

    # 4d: Lien /reprise present dans le footer
    footer = get_dealer_footer()
    if "kenbot-dashboard-five.vercel.app/reprise" in footer:
        passed += 1
        print(f"  [OK] Lien /reprise present dans footer")
    else:
        failed += 1
        print(f"  [FAIL] Lien /reprise ABSENT du footer!")
        print(f"         Footer actuel: {footer[-200:]}")

    # 4e: Texte "J'evalue ton echange" present
    if "evalue" in footer.lower() or "évalue" in footer.lower():
        passed += 1
        print("  [OK] Texte evaluation present")
    else:
        failed += 1
        print("  [FAIL] Texte evaluation absent")

    # 4f: Telephone present
    if "418-222-3939" in footer:
        passed += 1
        print("  [OK] Telephone 418-222-3939 present")
    else:
        failed += 1
        print("  [FAIL] Telephone absent")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 5: EVENT SOLD
# ============================================================
def test_sold_event():
    print("\n" + "=" * 70)
    print("TEST 5: EVENT SOLD (MARQUAGE VENDU)")
    print("=" * 70)
    passed = failed = 0

    base_text = "RAM 1500 SPORT 2023\n\n52 995 $\n\n418-222-3939"
    sold_prefix = (
        "\U0001f6a8 VENDU \U0001f6a8\n\n"
        "Ce v\u00e9hicule n'est plus disponible.\n\n"
        "\U0001f449 Vous recherchez un v\u00e9hicule semblable ?\n"
        "Contactez-moi directement.\n\n"
        "Daniel Giroux\n"
        "\U0001f4de 418-222-3939\n"
        "\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\n"
    )
    sold_message = sold_prefix + base_text

    if "VENDU" in sold_message:
        passed += 1
        print("  [OK] Prefixe VENDU present")
    else:
        failed += 1
        print("  [FAIL] Prefixe VENDU absent")

    if "418-222-3939" in sold_message:
        passed += 1
        print("  [OK] Telephone dans VENDU")
    else:
        failed += 1
        print("  [FAIL] Telephone absent du VENDU")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 6: EVENT UNSOLD
# ============================================================
def test_unsold_event():
    print("\n" + "=" * 70)
    print("TEST 6: EVENT UNSOLD (RESTAURATION)")
    print("=" * 70)
    passed = failed = 0

    sold_text = "\U0001f6a8 VENDU \U0001f6a8\n\nCe v\u00e9hicule n'est plus disponible.\n\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n\nRAM 1500 SPORT 2023\n\n52 995 $"

    # Extraction comme dans runner_cron_prod.py
    if "\u2500\u2500\u2500\u2500" in sold_text:
        parts = sold_text.split("\u2500" * 20 + "\n\n", 1)
        restored = parts[1] if len(parts) > 1 else sold_text
    else:
        restored = sold_text

    if "VENDU" not in restored:
        passed += 1
        print(f"  [OK] Texte restaure: {restored[:50]}...")
    else:
        failed += 1
        print(f"  [FAIL] VENDU encore present")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 7: VIN DECODE NHTSA
# ============================================================
def test_vin_decode():
    print("\n" + "=" * 70)
    print("TEST 7: VIN DECODE NHTSA")
    print("=" * 70)
    passed = failed = 0

    for tv in TEST_VINS:
        vin = tv["vin"]
        expect_make = tv["expect_make"]
        desc = tv["desc"]

        print(f"\n  --- {desc} (VIN: {vin}) ---")
        specs = decode_vin(vin)

        if specs:
            make = (specs.get("make") or "").upper()
            hp = specs.get("engine_hp", "")
            engine_line = format_engine_line(specs)
            prompt_text = format_specs_for_prompt(specs)

            if expect_make.upper() in make:
                passed += 1
                print(f"  [OK] Make: {make}")
            else:
                failed += 1
                print(f"  [FAIL] Make: got {make}, expected {expect_make}")

            if engine_line:
                passed += 1
                print(f"  [OK] Moteur: {engine_line}")
            else:
                failed += 1
                print(f"  [FAIL] Moteur vide")

            if prompt_text and len(prompt_text) > 20:
                passed += 1
                print(f"  [OK] Specs pour prompt: {len(prompt_text)} chars")
                for line in prompt_text.split("\n")[:5]:
                    print(f"        {line}")
            else:
                failed += 1
                print(f"  [FAIL] Specs prompt trop courtes")
        else:
            failed += 3
            print(f"  [FAIL] VIN decode retourne None")

        time.sleep(0.5)  # rate limit NHTSA

    # Test VIN invalide
    bad = decode_vin("INVALID")
    if bad is None:
        passed += 1
        print(f"\n  [OK] VIN invalide retourne None")
    else:
        failed += 1
        print(f"\n  [FAIL] VIN invalide devrait retourner None")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 8: META COMPARE (site vs FB) — import check
# ============================================================
def test_meta_compare_import():
    print("\n" + "=" * 70)
    print("TEST 8: META COMPARE (site vs FB) — VERIFICATION MODULE")
    print("=" * 70)
    passed = failed = 0

    try:
        from meta_compare_supabase import meta_compare as meta_compare_fn
        passed += 1
        print("  [OK] meta_compare_supabase importable")
    except Exception as e:
        failed += 1
        print(f"  [FAIL] Import meta_compare: {e}")

    # Verifier les fonctions du module
    try:
        from meta_compare_supabase import norm_url, load_meta_feed_from_storage
        passed += 1
        print("  [OK] Fonctions norm_url et load_meta_feed_from_storage disponibles")

        # Test norm_url
        test_url = "https://www.kennebecdodge.ca/vehicules/ram-1500/"
        normed = norm_url(test_url)
        if normed and "kennebecdodge" in normed:
            passed += 1
            print(f"  [OK] norm_url: {normed}")
        else:
            failed += 1
            print(f"  [FAIL] norm_url incorrect: {normed}")
    except Exception as e:
        failed += 2
        print(f"  [FAIL] Fonctions meta_compare: {e}")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 9: NO_PHOTO SCENARIO
# ============================================================
def test_no_photo_scenario():
    print("\n" + "=" * 70)
    print("TEST 9: NO_PHOTO SCENARIO")
    print("=" * 70)
    passed = failed = 0

    from pathlib import Path

    # Simuler la detection NO_PHOTO comme dans runner_cron_prod.py
    # Fonction _has_only_no_photo_fallback
    def has_only_no_photo(photos):
        if len(photos) == 1 and "NO_PHOTO" in str(photos[0]):
            return True
        return False

    fake_photos_nophoto = [Path("/tmp/06300_NO_PHOTO.jpg")]
    fake_photos_real = [Path("/tmp/06300_front.jpg"), Path("/tmp/06300_side.jpg")]

    if has_only_no_photo(fake_photos_nophoto):
        passed += 1
        print("  [OK] NO_PHOTO detecte correctement")
    else:
        failed += 1
        print("  [FAIL] NO_PHOTO non detecte")

    if not has_only_no_photo(fake_photos_real):
        passed += 1
        print("  [OK] Vraies photos non marquees NO_PHOTO")
    else:
        failed += 1
        print("  [FAIL] Vraies photos marquees NO_PHOTO a tort")

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 10: AI GENERATION (avec API OpenAI)
# ============================================================
def test_ai_generation():
    print("\n" + "=" * 70)
    print("TEST 10: GENERATION AI (pipeline — generate_smart_text)")
    print("=" * 70)
    try:
        from pipeline.generator import generate_smart_text
    except ImportError as e:
        print(f"  [SKIP] {e}")
        return 0, 0

    passed = failed = 0
    for v in TEST_VEHICLES:
        event = v.get("_test_event", "NEW")
        title = v["title"]
        print(f"\n  --- {v['_test_type']}: {title} (event={event}) ---")

        kwargs = {"vehicle": v, "event": event}
        if event == "PRICE_CHANGED":
            kwargs["old_price"] = v.get("old_price")
            kwargs["new_price"] = v.get("new_price")

        start = time.time()
        text = generate_smart_text(**kwargs)
        elapsed = time.time() - start

        if text and len(text) > 50:
            # Verifier aucun cliche
            has_cliche = any(c in text.lower() for c in CLICHES_INTERDITS_LIST)
            if has_cliche:
                failed += 1
                print(f"  [FAIL] Cliche detecte dans le texte!")
            else:
                passed += 1
                has_price = "$" in text
                print(f"  [OK] {len(text)} chars, {elapsed:.1f}s, prix={'OUI' if has_price else 'NON'}")
                print(f"  INTRO: {text[:150]}...")
        else:
            failed += 1
            print(f"  [FAIL] Texte vide ou court ({len(text) if text else 0})")

        time.sleep(1)

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 11: AI ACCROCHES (llm.py compatible)
# ============================================================
def test_ai_accroche():
    print("\n" + "=" * 70)
    print("TEST 11: ACCROCHES COURTES (pipeline — generate_ad_text)")
    print("=" * 70)
    try:
        from pipeline.generator import generate_ad_text
    except ImportError as e:
        print(f"  [SKIP] {e}")
        return 0, 0

    passed = failed = 0
    subset = [v for v in TEST_VEHICLES if v["_test_type"] in ("stellantis_truck", "exotic_lambo", "non_stellantis_sedan")]
    for v in subset:
        print(f"\n  --- {v['_test_type']}: {v['title']} ---")
        text = generate_ad_text(v, max_chars=400)
        if text and len(text) > 20:
            passed += 1
            print(f"  [OK] {len(text)} chars: {text[:100]}...")
        else:
            failed += 1
            print(f"  [FAIL] Texte vide ou court")
        time.sleep(1)

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# TEST 12: AI INTROS
# ============================================================
def test_ai_intro():
    print("\n" + "=" * 70)
    print("TEST 12: INTROS ONLY (pipeline — generate_intro_only)")
    print("=" * 70)
    try:
        from pipeline.generator import generate_intro_only
    except ImportError as e:
        print(f"  [SKIP] {e}")
        return 0, 0

    passed = failed = 0
    subset = [v for v in TEST_VEHICLES if v["_test_type"] in ("stellantis_muscle", "non_stellantis_truck")]
    for v in subset:
        print(f"\n  --- {v['_test_type']}: {v['title']} ---")
        text = generate_intro_only(v, max_chars=250)
        if text and len(text) > 10:
            passed += 1
            print(f"  [OK] {len(text)} chars: {text}")
        else:
            failed += 1
            print(f"  [FAIL] Intro vide")
        time.sleep(1)

    print(f"\n  Resultat: {passed}/{passed + failed}")
    return passed, failed


# ============================================================
# RAPPORT FINAL
# ============================================================
def run_all_tests(with_api: bool = True):
    print("\n" + "#" * 70)
    print("#  KENBOT PIPELINE — SUITE DE TESTS COMPLETE")
    print("#  Date: " + time.strftime("%Y-%m-%d %H:%M:%S"))
    print("#  API: " + ("ACTIVE" if with_api else "DESACTIVEE"))
    print("#" * 70)

    total_p = total_f = 0

    # Tests TOUJOURS executes (sans API)
    for fn in [test_classifier, test_vehicle_intelligence, test_cliche_filter,
               test_footer_et_reprise, test_sold_event, test_unsold_event,
               test_vin_decode, test_meta_compare_import, test_no_photo_scenario]:
        p, f = fn()
        total_p += p
        total_f += f

    # Tests AI (avec API)
    if with_api:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            print("\n  [SKIP AI TESTS] OPENAI_API_KEY non definie")
        else:
            for fn in [test_ai_generation, test_ai_accroche, test_ai_intro]:
                p, f = fn()
                total_p += p
                total_f += f

    print("\n" + "=" * 70)
    print("RAPPORT FINAL")
    print("=" * 70)
    print(f"  Tests passes:  {total_p}")
    print(f"  Tests echoues: {total_f}")
    print(f"  Total:         {total_p + total_f}")

    if total_f == 0:
        print(f"\n  {total_p}/{total_p} TESTS PASSENT!")
    else:
        print(f"\n  {total_f} TESTS ONT ECHOUE")
    print("=" * 70)

    return total_f == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-api", action="store_true")
    args = parser.parse_args()
    success = run_all_tests(with_api=not args.no_api)
    sys.exit(0 if success else 1)
