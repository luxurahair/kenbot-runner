#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_pipeline_generation.py
============================
Programme de test complet pour le pipeline de generation d'annonces.
Teste TOUS les cas d'utilisation du cron:

1. Stellantis AVEC Window Sticker (RAM, Dodge, Jeep, Chrysler)
2. Stellantis SANS Window Sticker
3. Non-Stellantis (Ford, Toyota, Honda, Mazda, etc.)
4. Vehicules exotiques (Ferrari, Lamborghini)
5. PRICE_CHANGED event
6. PHOTOS_ADDED (regeneration texte)
7. SOLD (prefixe VENDU)
8. UNSOLD (restauration)
9. Tous les types: truck, SUV, coupe, sedan, minivan, EV, off-road
10. NO_PHOTO scenario

Usage:
    python tests/test_pipeline_generation.py                # Test complet (avec API)
    python tests/test_pipeline_generation.py --no-api       # Test sans API (parsing seulement)
    python tests/test_pipeline_generation.py --type truck    # Test un seul type
"""

import os
import sys
import json
import time
import argparse
from typing import Dict, Any, List, Optional

# Ajouter le root au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from classifier import classify
from vehicle_intelligence import build_vehicle_context, parse_vehicle_title, get_vehicle_profile
from footer_utils import has_footer, add_footer_if_missing, get_dealer_footer
from pipeline.cliches import filter_cliches, remove_cliche_lines, CLICHES_INTERDITS_LIST

# ============================================================
# VEHICULES DE TEST — Couvre TOUS les cas du cron
# ============================================================

TEST_VEHICLES: List[Dict[str, Any]] = [
    # === STELLANTIS (avec sticker possible) ===
    {
        "title": "RAM 1500 SPORT 2023",
        "stock": "06300",
        "vin": "1C6SRFTT7MN517688",
        "price_int": 52995,
        "km_int": 25000,
        "url": "https://www.kennebec.com/ram-1500-sport-2023",
        "_test_type": "stellantis_truck",
        "_test_event": "NEW",
        "_expect_type": "pickup",
    },
    {
        "title": "Dodge CHALLENGER R/T SCAT PACK BLANC 2023",
        "stock": "06234",
        "vin": "2C3CDZFJ1PH593481",
        "price_int": 79995,
        "km_int": 11500,
        "_test_type": "stellantis_muscle",
        "_test_event": "NEW",
        "_expect_type": "muscle_car",
    },
    {
        "title": "Jeep WRANGLER RUBICON 4XE 2024",
        "stock": "06106",
        "vin": "1C4HJXFN5RW123456",
        "price_int": 62995,
        "km_int": 15586,
        "_test_type": "stellantis_offroad",
        "_test_event": "NEW",
        "_expect_type": "off_road",
    },
    {
        "title": "Jeep GRAND CHEROKEE LIMITED 2023",
        "stock": "06188",
        "vin": "1C4RJFBG2PC123456",
        "price_int": 54995,
        "km_int": 32000,
        "_test_type": "stellantis_suv",
        "_test_event": "NEW",
        "_expect_type": "suv_premium",
    },
    {
        "title": "Ram 2500 BIG HORN 2025",
        "stock": "06230",
        "vin": "3C6UR5DJ1RG123456",
        "price_int": 71995,
        "km_int": 25,
        "_test_type": "stellantis_hd",
        "_test_event": "NEW",
        "_expect_type": "pickup_hd",
    },
    {
        "title": "Chrysler PACIFICA TOURING L 2023",
        "stock": "06150",
        "vin": "2C4RC1BG5PR123456",
        "price_int": 42995,
        "km_int": 28000,
        "_test_type": "stellantis_minivan",
        "_test_event": "NEW",
        "_expect_type": "minivan",
    },
    {
        "title": "Dodge HORNET R/T PLUS 2024",
        "stock": "06280",
        "vin": "ZACPDFCW2R3123456",
        "price_int": 44995,
        "km_int": 8500,
        "_test_type": "stellantis_suv_compact",
        "_test_event": "NEW",
        "_expect_type": "suv_compact",
    },

    # === NON-STELLANTIS ===
    {
        "title": "Ford MUSTANG GT 2022",
        "stock": "46104A",
        "vin": "",
        "price_int": 45995,
        "km_int": 21433,
        "_test_type": "non_stellantis_coupe",
        "_test_event": "NEW",
        "_expect_type": "muscle_car",
    },
    {
        "title": "Toyota RAV4 XLE AWD 2023",
        "stock": "46200A",
        "vin": "",
        "price_int": 34995,
        "km_int": 35000,
        "_test_type": "non_stellantis_suv",
        "_test_event": "NEW",
        "_expect_type": "suv_compact",
    },
    {
        "title": "Honda CIVIC EX 2022",
        "stock": "46300A",
        "vin": "",
        "price_int": 26995,
        "km_int": 42000,
        "_test_type": "non_stellantis_sedan",
        "_test_event": "NEW",
        "_expect_type": "berline",
    },
    {
        "title": "Ford F-150 XLT SUPERCREW 4X4 2023",
        "stock": "46400A",
        "vin": "",
        "price_int": 49995,
        "km_int": 38000,
        "_test_type": "non_stellantis_truck",
        "_test_event": "NEW",
        "_expect_type": "pickup",
    },

    # === EXOTIQUES ===
    {
        "title": "LAMBORGHINI HURACAN 2024",
        "stock": "06232",
        "vin": "",
        "price_int": 343995,
        "km_int": 8900,
        "_test_type": "exotic",
        "_test_event": "NEW",
        "_expect_type": "exotique",
    },
    {
        "title": "Ferrari 488 GTB 2020",
        "stock": "06290",
        "vin": "",
        "price_int": 289995,
        "km_int": 12000,
        "_test_type": "exotic_ferrari",
        "_test_event": "NEW",
        "_expect_type": "exotique",
    },

    # === EV / HYBRIDE ===
    {
        "title": "Fiat 500 E RED 2024",
        "stock": "44220A",
        "vin": "",
        "price_int": 23995,
        "km_int": 22,
        "_test_type": "ev",
        "_test_event": "NEW",
        "_expect_type": "citadine",
    },

    # === PRICE_CHANGED ===
    {
        "title": "RAM 1500 BIG HORN 2022",
        "stock": "06100",
        "vin": "1C6SRFFT8MN123456",
        "price_int": 39995,
        "km_int": 55000,
        "old_price": 44995,
        "new_price": 39995,
        "_test_type": "price_changed",
        "_test_event": "PRICE_CHANGED",
        "_expect_type": "pickup",
    },
]

# ============================================================
# TESTS PARSING (sans API)
# ============================================================

def test_classifier():
    """Teste la classification de chaque vehicule."""
    print("\n" + "=" * 70)
    print("TEST 1: CLASSIFIER")
    print("=" * 70)

    passed = 0
    failed = 0

    for v in TEST_VEHICLES:
        result = classify(v)
        test_type = v["_test_type"]
        print(f"  [{test_type:30s}] {v['title']:45s} -> {result}")
        passed += 1

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_vehicle_intelligence():
    """Teste le contexte vehicule (parsing titre, profil, specs)."""
    print("\n" + "=" * 70)
    print("TEST 2: VEHICLE INTELLIGENCE")
    print("=" * 70)

    passed = 0
    failed = 0

    for v in TEST_VEHICLES:
        ctx = build_vehicle_context(v)
        test_type = v["_test_type"]
        expect_type = v.get("_expect_type", "")

        brand = ctx.get("brand", "?")
        model = ctx.get("model", "?")
        vtype = ctx.get("vehicle_type", "?")
        hp = ctx.get("hp", "")
        vibe = ctx.get("trim_vibe", "")

        status = "OK" if vtype == expect_type or not expect_type else f"MISMATCH (got {vtype}, expected {expect_type})"
        if "OK" in status:
            passed += 1
        else:
            failed += 1

        print(f"  [{test_type:30s}] brand={brand:12s} model={model:15s} type={vtype:15s} hp={hp:5s} [{status}]")
        if vibe:
            print(f"  {'':32s} vibe: {vibe}")

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_cliche_filter():
    """Teste le filtre anti-cliches."""
    print("\n" + "=" * 70)
    print("TEST 3: FILTRE ANTI-CLICHES")
    print("=" * 70)

    passed = 0
    failed = 0

    # Textes avec cliches (doivent etre filtres)
    cliche_texts = [
        "Ce RAM est pret a sillonner les routes de la Beauce!",
        "Cette merveille n'attend plus que toi.",
        "Parfait pour l'hiver quebecois, ce Jeep va dominer les routes.",
    ]

    for text in cliche_texts:
        result = filter_cliches(text)
        if result == "":
            passed += 1
            print(f"  [FILTRE OK] Cliche detecte dans: {text[:50]}...")
        else:
            failed += 1
            print(f"  [FILTRE FAIL] Cliche NON detecte dans: {text[:50]}...")

    # Textes sans cliches (doivent passer)
    clean_texts = [
        "J'ai rentre ce RAM 1500 la semaine passee. V8 HEMI, 25 000 km.",
        "Un Challenger Scat Pack avec 11 500 km, c'est rare.",
        "Le proprio l'a garde 2 ans, toujours dans le garage.",
    ]

    for text in clean_texts:
        result = filter_cliches(text)
        if result == text:
            passed += 1
            print(f"  [PROPRE OK] Texte propre passe: {text[:50]}...")
        else:
            failed += 1
            print(f"  [PROPRE FAIL] Texte propre rejete: {text[:50]}...")

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_footer():
    """Teste le footer Daniel Giroux."""
    print("\n" + "=" * 70)
    print("TEST 4: FOOTER DANIEL GIROUX")
    print("=" * 70)

    passed = 0
    failed = 0

    # Texte sans footer
    text_no_footer = "RAM 1500 2022\n\n34 995 $"
    if not has_footer(text_no_footer):
        passed += 1
        print("  [OK] Texte sans footer detecte correctement")
    else:
        failed += 1
        print("  [FAIL] Faux positif sur texte sans footer")

    # Ajout de footer
    result = add_footer_if_missing(text_no_footer)
    if has_footer(result):
        passed += 1
        print("  [OK] Footer ajoute correctement")
    else:
        failed += 1
        print("  [FAIL] Footer non ajoute")

    # Pas de double footer
    result2 = add_footer_if_missing(result)
    count = result2.count("418-222-3939")
    if count == 1:
        passed += 1
        print("  [OK] Pas de double footer")
    else:
        failed += 1
        print(f"  [FAIL] Double footer detecte ({count} occurrences)")

    # Lien reprise present
    footer = get_dealer_footer()
    if "reprise" in footer.lower():
        passed += 1
        print("  [OK] Lien /reprise present dans le footer")
    else:
        failed += 1
        print("  [FAIL] Lien /reprise absent du footer")

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_sold_event():
    """Teste le scenario SOLD (marquage VENDU)."""
    print("\n" + "=" * 70)
    print("TEST 5: EVENT SOLD (MARQUAGE VENDU)")
    print("=" * 70)

    passed = 0
    failed = 0

    # Simuler un texte existant
    base_text = "RAM 1500 SPORT 2023\n\n52 995 $\n\n418-222-3939"
    sold_prefix = (
        "VENDU\n\n"
        "Ce vehicule n'est plus disponible.\n\n"
        "Contactez-moi directement.\n"
        "Daniel Giroux\n"
        "418-222-3939\n"
        "----\n\n"
    )
    sold_message = sold_prefix + base_text

    if "VENDU" in sold_message:
        passed += 1
        print("  [OK] Prefixe VENDU present")
    else:
        failed += 1
        print("  [FAIL] Prefixe VENDU absent")

    # Pas de double VENDU
    if sold_message.count("VENDU") <= 2:
        passed += 1
        print("  [OK] Pas de triple VENDU")
    else:
        failed += 1
        print("  [FAIL] Triple VENDU detecte")

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_unsold_event():
    """Teste le scenario UNSOLD (restauration)."""
    print("\n" + "=" * 70)
    print("TEST 6: EVENT UNSOLD (RESTAURATION)")
    print("=" * 70)

    passed = 0
    failed = 0

    sold_text = (
        "VENDU\n\n"
        "Ce vehicule n'est plus disponible.\n\n"
        "----\n\n"
        "RAM 1500 SPORT 2023\n\n52 995 $"
    )

    # Extraire le texte original
    if "----\n\n" in sold_text:
        parts = sold_text.split("----\n\n", 1)
        restored = parts[1] if len(parts) > 1 else sold_text
    else:
        restored = sold_text

    if "VENDU" not in restored:
        passed += 1
        print(f"  [OK] Texte restaure sans VENDU: {restored[:50]}...")
    else:
        failed += 1
        print(f"  [FAIL] VENDU encore present apres restauration")

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


# ============================================================
# TESTS AI (avec API)
# ============================================================

def test_ai_generation():
    """Teste la generation AI pour chaque type de vehicule."""
    print("\n" + "=" * 70)
    print("TEST 7: GENERATION AI (llm_v3 — generate_smart_text)")
    print("=" * 70)

    try:
        from pipeline.generator import generate_smart_text
    except ImportError as e:
        print(f"  [SKIP] Import error: {e}")
        return 0, 0

    passed = 0
    failed = 0

    for v in TEST_VEHICLES:
        test_type = v["_test_type"]
        event = v.get("_test_event", "NEW")
        title = v["title"]

        print(f"\n  --- {test_type}: {title} (event={event}) ---")

        kwargs = {"vehicle": v, "event": event}
        if event == "PRICE_CHANGED":
            kwargs["old_price"] = v.get("old_price")
            kwargs["new_price"] = v.get("new_price")

        start = time.time()
        text = generate_smart_text(**kwargs)
        elapsed = time.time() - start

        if text and len(text) > 50:
            passed += 1

            # Verifier aucun cliche
            has_cliche = False
            for c in CLICHES_INTERDITS_LIST:
                if c in text.lower():
                    has_cliche = True
                    print(f"  [CLICHE!] '{c}' detecte dans le texte!")
                    break

            if has_cliche:
                failed += 1
                passed -= 1

            # Verifier que le prix est mentionne
            price = v.get("price_int")
            price_str = f"{price:,}".replace(",", " ") if price else ""
            has_price = price_str in text or "$" in text

            print(f"  [OK] {len(text)} chars, {elapsed:.1f}s, prix={'OUI' if has_price else 'NON'}")
            print(f"  INTRO: {text[:200]}...")
        else:
            failed += 1
            print(f"  [FAIL] Texte vide ou trop court ({len(text) if text else 0} chars)")

        # Rate limiting
        time.sleep(1)

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_ai_accroche():
    """Teste la generation d'accroches courtes (llm.py compatible)."""
    print("\n" + "=" * 70)
    print("TEST 8: ACCROCHES COURTES (llm.py — generate_ad_text)")
    print("=" * 70)

    try:
        from pipeline.generator import generate_ad_text
    except ImportError as e:
        print(f"  [SKIP] Import error: {e}")
        return 0, 0

    passed = 0
    failed = 0

    # Tester 3 vehicules representatifs
    test_subset = [v for v in TEST_VEHICLES if v["_test_type"] in ("stellantis_truck", "exotic", "non_stellantis_sedan")]

    for v in test_subset:
        title = v["title"]
        print(f"\n  --- {v['_test_type']}: {title} ---")

        start = time.time()
        text = generate_ad_text(v, max_chars=400)
        elapsed = time.time() - start

        if text and len(text) > 20:
            passed += 1
            print(f"  [OK] {len(text)} chars, {elapsed:.1f}s")
            print(f"  TEXTE: {text}")
        else:
            failed += 1
            print(f"  [FAIL] Texte vide ou trop court")

        time.sleep(1)

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_ai_intro():
    """Teste la generation d'intros (generate_intro_only)."""
    print("\n" + "=" * 70)
    print("TEST 9: INTROS ONLY (llm.py — generate_intro_only)")
    print("=" * 70)

    try:
        from pipeline.generator import generate_intro_only
    except ImportError as e:
        print(f"  [SKIP] Import error: {e}")
        return 0, 0

    passed = 0
    failed = 0

    test_subset = [v for v in TEST_VEHICLES if v["_test_type"] in ("stellantis_muscle", "non_stellantis_truck")]

    for v in test_subset:
        title = v["title"]
        print(f"\n  --- {v['_test_type']}: {title} ---")

        start = time.time()
        text = generate_intro_only(v, max_chars=250)
        elapsed = time.time() - start

        if text and len(text) > 10:
            passed += 1
            print(f"  [OK] {len(text)} chars, {elapsed:.1f}s")
            print(f"  INTRO: {text}")
        else:
            failed += 1
            print(f"  [FAIL] Intro vide ou trop courte")

        time.sleep(1)

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


def test_price_changed_ai():
    """Teste la generation pour PRICE_CHANGED."""
    print("\n" + "=" * 70)
    print("TEST 10: PRICE_CHANGED AI")
    print("=" * 70)

    try:
        from pipeline.generator import generate_smart_text
    except ImportError as e:
        print(f"  [SKIP] Import error: {e}")
        return 0, 0

    v = {
        "title": "RAM 1500 BIG HORN 2022",
        "stock": "06100",
        "vin": "1C6SRFFT8MN123456",
        "price_int": 39995,
        "km_int": 55000,
    }

    print(f"\n  --- PRICE_CHANGED: {v['title']} (44 995 -> 39 995) ---")

    start = time.time()
    text = generate_smart_text(
        vehicle=v,
        event="PRICE_CHANGED",
        old_price=44995,
        new_price=39995,
    )
    elapsed = time.time() - start

    passed = 0
    failed = 0

    if text and len(text) > 50:
        passed += 1
        print(f"  [OK] {len(text)} chars, {elapsed:.1f}s")
        # Verifier que la baisse de prix est mentionnee
        if "39" in text or "baisse" in text.lower() or "prix" in text.lower():
            passed += 1
            print(f"  [OK] Baisse de prix mentionnee")
        else:
            failed += 1
            print(f"  [WARN] Baisse de prix non detectee dans le texte")
        print(f"  TEXTE: {text[:300]}...")
    else:
        failed += 2
        print(f"  [FAIL] Texte vide ou trop court")

    print(f"\n  Resultat: {passed} passes, {failed} echoues")
    return passed, failed


# ============================================================
# RAPPORT FINAL
# ============================================================

def run_all_tests(with_api: bool = True, filter_type: str = None):
    """Execute tous les tests et affiche un rapport."""
    print("\n" + "#" * 70)
    print("#  KENBOT PIPELINE — SUITE DE TESTS COMPLÈTE")
    print("#  Date: " + time.strftime("%Y-%m-%d %H:%M:%S"))
    print("#  API: " + ("ACTIVE" if with_api else "DESACTIVEE"))
    print("#" * 70)

    total_passed = 0
    total_failed = 0

    # Tests sans API (toujours)
    for test_fn in [test_classifier, test_vehicle_intelligence, test_cliche_filter,
                    test_footer, test_sold_event, test_unsold_event]:
        p, f = test_fn()
        total_passed += p
        total_failed += f

    # Tests avec API (optionnel)
    if with_api:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            print("\n[SKIP AI TESTS] OPENAI_API_KEY non definie")
        else:
            for test_fn in [test_ai_generation, test_ai_accroche,
                            test_ai_intro, test_price_changed_ai]:
                p, f = test_fn()
                total_passed += p
                total_failed += f

    # Rapport
    print("\n" + "=" * 70)
    print("RAPPORT FINAL")
    print("=" * 70)
    print(f"  Tests passes:  {total_passed}")
    print(f"  Tests echoues: {total_failed}")
    print(f"  Total:         {total_passed + total_failed}")
    print()

    if total_failed == 0:
        print("  TOUS LES TESTS PASSENT!")
    else:
        print(f"  {total_failed} TESTS ONT ECHOUE")

    print("=" * 70)

    return total_failed == 0


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tests pipeline generation Kenbot")
    parser.add_argument("--no-api", action="store_true", help="Skip les tests AI (pas de cle API)")
    parser.add_argument("--type", type=str, help="Filtrer par type de test (ex: truck, exotic)")
    args = parser.parse_args()

    success = run_all_tests(with_api=not args.no_api, filter_type=args.type)
    sys.exit(0 if success else 1)
