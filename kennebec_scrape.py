import re
import requests
import json
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Set, Optional
from urllib.parse import urljoin, urlsplit, urlunsplit

def _clean_price_int(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d[\d\s.,]{2,})\s*\$", s)
    if not m:
        return None
    n = re.sub(r"[^\d]", "", m.group(1))
    try:
        return int(n)
    except Exception:
        return None

def _clean_km_int(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d[\d\s.,]{2,})\s*km", s.lower())
    if not m:
        return None
    n = re.sub(r"[^\d]", "", m.group(1))
    try:
        return int(n)
    except Exception:
        return None

def slugify(title: str, stock: str) -> str:
    base = (title or "").lower()
    base = re.sub(r"[^a-z0-9]+", "-", base)
    base = re.sub(r"-+", "-", base).strip("-")
    stock = (stock or "").strip().upper()
    return f"{base}-{stock.lower()}"

def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

def parse_inventory_listing_urls(base_url: str, inventory_path: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: Set[str] = set()

    def add(u: str) -> None:
        if not u:
            return
        full = u if u.startswith("http") else urljoin(base_url, u)
        parts = urlsplit(full)
        path = parts.path or ""
        if not path.startswith(inventory_path):
            return
        if not re.search(r"-id\d+$", path.rstrip("/"), re.IGNORECASE):
            return
        clean = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        out.add(clean)

    for a in soup.find_all("a", href=True):
        add(a.get("href") or "")

    # Regex dynamique basée sur inventory_path (supporte neuf ET occasion)
    path_for_regex = re.escape(inventory_path.rstrip("/"))
    pattern = rf'({path_for_regex}/[^\s"\'<>]+?-id\d+)'
    for m in re.findall(pattern, html, flags=re.IGNORECASE):
        add(m)

    return sorted(out)

def _uniq_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        s = " ".join(str(x).split()).strip()
        if not s:
            continue
        k = s.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def _extract_features_from_html(html: str) -> Dict[str, Any]:
    """
    Essaie d'extraire features/comfort/specs depuis des JSON embarqués.
    Kennebec/Dilawri met souvent un gros objet 'vehicleDetails' dans la page.
    """
    out: Dict[str, Any] = {"features": [], "comfort": [], "specs": {}}

    # 1) Cherche un bloc JSON "vehicleDetails": {...}
    m = re.search(r'vehicleDetails"\s*:\s*({.*?})\s*,\s*"isNew"', html, flags=re.DOTALL)
    if not m:
        # fallback: parfois 'vehicleDetails = {...};'
        m = re.search(r"vehicleDetails\s*=\s*({.*?})\s*;\s*", html, flags=re.DOTALL)

    if not m:
        return out

    raw = m.group(1)

    # on tente un json.loads après nettoyage léger
    try:
        # remplace les guillemets simples si c'est du JSON "presque"
        # (si ça pète, on retourne vide)
        data = json.loads(raw)
    except Exception:
        return out

    # mapping le plus courant
    specs = data.get("specs") or data.get("specifications") or {}
    if isinstance(specs, dict):
        out["specs"] = {str(k): str(v) for k, v in specs.items() if v}

    feats = data.get("features") or data.get("options") or []
    if isinstance(feats, list):
        out["features"] = _uniq_keep_order(feats)

    comfort = data.get("comfort") or data.get("comfortFeatures") or []
    if isinstance(comfort, list):
        out["comfort"] = _uniq_keep_order(comfort)

    return out


def _extract_list_near_heading(soup: BeautifulSoup, keywords: List[str]) -> List[str]:
    """
    Fallback HTML: cherche un titre contenant un keyword, puis récupère les <li> proches.
    """
    keys = [k.lower() for k in keywords]
    for node in soup.find_all(["h2", "h3", "h4", "div", "span"]):
        t = " ".join(node.get_text(" ", strip=True).split()).lower()
        if any(k in t for k in keys):
            parent = node.parent
            lis = parent.find_all("li")
            items = []
            for li in lis:
                s = " ".join(li.get_text(" ", strip=True).split()).strip()
                if s:
                    items.append(s)
            return _uniq_keep_order(items)
    return []

def _extract_headline_line(soup: BeautifulSoup) -> str:
    """
    Ligne juste sous le H1, souvent du style '*AWD*TOIT PANO*...*'
    """
    h1 = soup.find("h1")
    if not h1:
        return ""
    # regarde les 1-3 prochains blocs texte
    for sib in h1.find_all_next(["p", "div", "h2", "span"], limit=6):
        txt = sib.get_text(" ", strip=True)
        if not txt:
            continue
        # on veut la ligne avec plein d'étoiles / séparateurs
        if txt.count("*") >= 4 or ("•" in txt) or ("|" in txt):
            return txt.strip()
        # parfois c'est sans étoiles mais très “liste”
        if len(txt) > 25 and txt.count(" ") <= 6 and txt.count("-") == 0:
            return txt.strip()
    return ""


def _extract_specs_dict(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Extrait le tableau "Spécifications" en dict {label: value}.
    Sur Kennebec, c'est souvent une grille 2 colonnes.
    """
    wanted = {
        "Transmission": "transmission",
        "Kilométrage": "kilometrage",
        "Chassis": "chassis",
        "Passagers": "passagers",
        "Cylindres": "cylindres",
        "Entraînement": "entrainement",
        "Entrainement": "entrainement",
        "Inventaire #": "inventaire",
        "Carburant": "carburant",
        "Couleur ext.": "couleur_ext",
        "Couleur int.": "couleur_int",
    }

    specs: Dict[str, str] = {}

    # stratégie: trouver l'endroit où il y a "Spécifications"
    anchor = None
    for n in soup.find_all(["h2", "h3", "h4", "div", "span"]):
        t = n.get_text(" ", strip=True).lower()
        if "spécification" in t or "specification" in t:
            anchor = n
            break

    if not anchor:
        return specs

    # Dans le bloc parent, prendre toutes les paires "Label:" -> "Valeur"
    container = anchor.parent
    texts = [x.get_text(" ", strip=True) for x in container.find_all(["div", "span", "p"], limit=200)]
    texts = [t for t in texts if t]

    # fallback si le parent est trop petit : élargir un peu
    if len(texts) < 10:
        container = anchor.find_parent(["section", "div"]) or container
        texts = [x.get_text(" ", strip=True) for x in container.find_all(["div", "span", "p"], limit=400)]
        texts = [t for t in texts if t]

    # parse en paires
    for i, t in enumerate(texts):
        tt = t.strip()
        if not tt.endswith(":"):
            continue
        label = tt[:-1].strip()
        key = wanted.get(label)
        if not key:
            continue
        # valeur = prochain texte non vide
        val = ""
        for j in range(i + 1, min(i + 6, len(texts))):
            cand = texts[j].strip()
            if cand and not cand.endswith(":"):
                val = cand
                break
        if val:
            specs[key] = val

    return specs
    
def parse_vehicle_detail_simple(session: requests.Session, url: str) -> Dict[str, Any]:
    """
    Enrichi : titre/price/km + photos sm360 + options (features/comfort/specs + headline)
    Stratégie:
      - parse HTML (sections, titres, listes <li>)
      - récupérer la ligne sous le H1 (headline_features)
      - récupérer le tableau Spécifications en dict
      - dédoublonner / normaliser
    """
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")

    def norm_txt(s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    def uniq_keep_order(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in items or []:
            x = norm_txt(x)
            if not x:
                continue
            k = x.casefold()
            if k in seen:
                continue
            seen.add(k)
            out.append(x)
        return out

    def extract_list_near_heading(keywords: List[str]) -> List[str]:
        keys = [k.lower() for k in keywords]
        for node in soup.find_all(["h2", "h3", "h4", "div", "span"]):
            t = norm_txt(node.get_text(" ", strip=True)).lower()
            if not t:
                continue
            if any(k in t for k in keys):
                parent = node.parent
                lis = parent.find_all("li")
                items = []
                for li in lis:
                    s = norm_txt(li.get_text(" ", strip=True))
                    if s:
                        items.append(s)
                items = uniq_keep_order(items)
                if items:
                    return items
        return []

    def extract_headline_line() -> str:
        """
        Ligne juste sous le H1, souvent '*AWD*TOIT PANO*...*'
        """
        h1 = soup.find("h1")
        if not h1:
            return ""
        # regarde quelques siblings/prochains blocs texte
        for sib in h1.find_all_next(["p", "div", "span"], limit=10):
            txt = norm_txt(sib.get_text(" ", strip=True))
            if not txt:
                continue
            # heuristique: beaucoup de *
            if txt.count("*") >= 4:
                return txt
            # ou des séparateurs
            if txt.count("•") >= 2 or txt.count("|") >= 2:
                return txt
        return ""

    def extract_specs_dict() -> Dict[str, str]:
        """
        Extrait le bloc 'Spécifications' en dict.
        Sur Kennebec: lignes du type 'Transmission:' puis valeur.
        """
        wanted = {
            "Transmission": "transmission",
            "Kilométrage": "kilometrage",
            "Chassis": "chassis",
            "Passagers": "passagers",
            "Cylindres": "cylindres",
            "Entraînement": "entrainement",
            "Entrainement": "entrainement",
            "Inventaire #": "inventaire",
            "Carburant": "carburant",
            "Couleur ext.": "couleur_ext",
            "Couleur int.": "couleur_int",
        }

        # trouve l’ancre "Spécifications"
        anchor = None
        for n in soup.find_all(["h2", "h3", "h4", "div", "span"]):
            t = norm_txt(n.get_text(" ", strip=True)).lower()
            if "spécification" in t or "specification" in t:
                anchor = n
                break
        if not anchor:
            return {}

        container = anchor.find_parent(["section", "div"]) or anchor.parent
        nodes = container.find_all(["div", "span", "p"], limit=400)
        texts = [norm_txt(x.get_text(" ", strip=True)) for x in nodes]
        texts = [t for t in texts if t]

        specs: Dict[str, str] = {}
        for i, t in enumerate(texts):
            if not t.endswith(":"):
                continue
            label = t[:-1].strip()
            key = wanted.get(label)
            if not key:
                continue

            val = ""
            for j in range(i + 1, min(i + 8, len(texts))):
                cand = texts[j].strip()
                if cand and not cand.endswith(":"):
                    val = cand
                    break
            if val:
                specs[key] = val

        return specs

    # --------------------
    # title
    # --------------------
    h1 = soup.find("h1")
    title = (h1.get_text(" ", strip=True) if h1 else "").strip() or "Sans titre"

    # headline line under title
    headline_features = extract_headline_line()

    # --------------------
    # stock / vin
    # --------------------
    stock = ""
    vin = ""

    m = re.search(r"stockNumber\s*[:=]\s*['\"]([A-Za-z0-9]+)['\"]", html, re.IGNORECASE)
    if m:
        stock = m.group(1).strip().upper()

    # Kennebec affiche parfois VIN # ... ; regex plus permissive
    m = re.search(r"\bvin\s*#?\s*[:=]?\s*['\"]?([A-HJ-NPR-Z0-9]{17})['\"]?", html, re.IGNORECASE)
    if m:
        vin = m.group(1).strip().upper()

    # fallback vin dans page visible (VIN # XXXXX)
    if not vin:
        m = re.search(r"\bVIN\s*#\s*([A-HJ-NPR-Z0-9]{17})\b", html, re.IGNORECASE)
        if m:
            vin = m.group(1).strip().upper()

    # --------------------
    # price / mileage
    # --------------------
    price = ""
    mileage = ""

    mp = re.search(r"displayedPrice\s*[:=]\s*['\"]([0-9]+(?:\.[0-9]+)?)['\"]", html, re.IGNORECASE)
    if mp:
        try:
            n = int(float(mp.group(1)))
            price = f"{n:,}".replace(",", " ") + " $"
        except Exception:
            pass

    mk = re.search(r"\bmileage\s*[:=]\s*['\"]([0-9]+(?:\.[0-9]+)?)['\"]", html, re.IGNORECASE)
    if mk:
        try:
            n = int(float(mk.group(1)))
            mileage = f"{n:,}".replace(",", " ") + " km"
        except Exception:
            pass

    # fallback visible price/km (au cas où)
    if not mileage:
        m = re.search(r"(\d[\d\s]{2,})\s*KM\b", html, re.IGNORECASE)
        if m:
            mileage = re.sub(r"\s+", " ", m.group(1)).strip() + " km"
    if not price:
        m = re.search(r"(\d[\d\s]{2,})\s*\$\b", html, re.IGNORECASE)
        if m:
            price = re.sub(r"\s+", " ", m.group(1)).strip() + " $"

    # --------------------
    # PDSF ("Était") — neufs uniquement. Prix affiché AVANT rabais manufacturier.
    # Pattern Kennebec CMS: <span>Était</span> ... <span ... aria-label="Ancien prix"> 65 662 $</span>
    # Un fallback texte est ajouté au cas où le markup change.
    # --------------------
    pdsf_int: Optional[int] = None
    # 1. Pattern HTML précis (aria-label)
    mpdsf = re.search(
        r'Était</span>\s*<span[^>]*aria-label=["\']Ancien prix["\'][^>]*>\s*([\d\s\u202f\xa0.,]+)\s*\$',
        html,
        re.IGNORECASE,
    )
    if not mpdsf:
        # 2. Fallback via aria-label seul
        mpdsf = re.search(
            r'aria-label=["\']Ancien prix["\'][^>]*>\s*([\d\s\u202f\xa0.,]+)\s*\$',
            html,
            re.IGNORECASE,
        )
    if not mpdsf:
        # 3. Fallback texte "Était" suivi d'un prix dans les 300 chars
        mpdsf = re.search(
            r"Était[^\d$]{1,300}?(\d[\d\s\u202f\xa0.,]{2,})\s*\$",
            html,
            re.IGNORECASE | re.DOTALL,
        )
    if mpdsf:
        raw_pdsf = re.sub(r"[^\d]", "", mpdsf.group(1))
        try:
            pdsf_int = int(raw_pdsf) if raw_pdsf else None
        except Exception:
            pdsf_int = None
    # Rabais = PDSF - prix affiché (seulement si PDSF > prix)
    current_price_int = _clean_price_int(price) if price else None
    rabais_int: Optional[int] = None
    if pdsf_int and current_price_int and pdsf_int > current_price_int:
        rabais_int = pdsf_int - current_price_int
    else:
        # Si pas de PDSF détecté, on ne crée pas de rabais artificiel.
        # Le routeur dashboard utilisera price_int comme PDSF de fallback côté CalcAuto.
        rabais_int = None

    # --------------------
    # photos sm360
    # --------------------
    # Détection multi-sources: <img src|data-src>, <img srcset>, meta og:image,
    # et fallback JSON-LD "image" field. Cap final à 10 photos uniques.
    photos: List[str] = []

    def _is_valid_vehicle_photo(u: str) -> bool:
        """True si l'URL semble être une photo de véhicule valide (pas un logo/thumbnail)."""
        if not u or len(u) < 20:
            return False
        low = u.lower()
        # Exclude logos, icons, thumbnails, placeholders (priority)
        if any(bad in low for bad in ("logo", "icon", "placeholder", "/ir/w75h23/", "/thumbnail/", "-thumb", "/logo-", "sticky-", "favicon")):
            return False
        # 1. Real inventory photos (dealership-taken)
        if "img.sm360.ca" in low and "/images/inventory/" in low:
            return True
        # 2. New-car catalog photos (manufacturer reference images — acceptable for
        #    freshly-arrived vehicles where the dealer has not taken real photos yet)
        if "img.sm360.ca" in low and "/images/newcar/" in low:
            return True
        # 3. Accept direct image URLs on the dealer site
        if "kennebecdodge.ca" in low and ("/content/inventory/" in low or "/content/newcar/" in low):
            return True
        # 4. Any URL with obvious vehicle-photo dimensions
        if any(tag in low for tag in ("/ir/w1280", "/ir/w1024", "/ir/w940", "/ir/w800")):
            if any(ext in low for ext in (".jpg", ".jpeg", ".png", ".webp")):
                return True
        return False

    # 1. <img> tags with src or data-src
    for img in soup.select("img"):
        for attr in ("data-src", "src", "data-lazy", "data-original"):
            val = img.get(attr)
            if val:
                u = val if val.startswith("http") else urljoin(url, val)
                if _is_valid_vehicle_photo(u):
                    photos.append(u)
                    break  # first valid URL for this img is enough

    # 2. <img srcset> — take the highest-resolution URL
    for img in soup.select("img[srcset]"):
        srcset = img.get("srcset", "")
        # Format: "url1 1x, url2 2x" OR "url1 400w, url2 800w"
        candidates = []
        for part in srcset.split(","):
            part = part.strip()
            if part:
                bits = part.split()
                if bits:
                    candidates.append(bits[0])
        if candidates:
            u = candidates[-1]  # last is usually highest-res
            if not u.startswith("http"):
                u = urljoin(url, u)
            if _is_valid_vehicle_photo(u):
                photos.append(u)

    # 3. Fallback: og:image meta if no photo found yet
    if not photos:
        og = soup.find("meta", attrs={"property": "og:image"})
        if og:
            u = og.get("content", "")
            if u:
                if not u.startswith("http"):
                    u = urljoin(url, u)
                if _is_valid_vehicle_photo(u):
                    photos.append(u)

    # 4. Fallback: JSON-LD image field (CarDealer schema)
    if not photos:
        for script in soup.select('script[type="application/ld+json"]'):
            try:
                import json as _json
                data = _json.loads(script.string or "{}")
            except Exception:
                continue
            if isinstance(data, list):
                data = data[0] if data else {}
            img_val = data.get("image") if isinstance(data, dict) else None
            if isinstance(img_val, str):
                if _is_valid_vehicle_photo(img_val):
                    photos.append(img_val)
            elif isinstance(img_val, list):
                for u in img_val:
                    if isinstance(u, str) and _is_valid_vehicle_photo(u):
                        photos.append(u)

    photos = uniq_keep_order(photos)
    # Cap at 10 (Facebook limit and consistency with UI upload)
    photos = photos[:10]

    # --------------------
    # features / comfort / specs
    # --------------------
    # HTML headings-based extraction (simple, stable)
    comfort = extract_list_near_heading(["Confort"])
    features = extract_list_near_heading(
        ["Équipements", "Equipements", "Caractéristiques", "Caracteristiques", "Options"]
    )

    features = uniq_keep_order(features)
    comfort = uniq_keep_order(comfort)

    # specs dict (Spécifications section)
    specs = extract_specs_dict()
    if not isinstance(specs, dict):
        specs = {}

    return {
        "url": url,
        "title": title,
        "headline_features": headline_features,
        "stock": stock,
        "vin": vin,
        "price": price,
        "mileage": mileage,
        "price_int": _clean_price_int(price) if price else None,
        "km_int": _clean_km_int(mileage) if mileage else None,
        "pdsf_int": pdsf_int,
        "rabais_int": rabais_int,
        "photos": photos,
        "features": features,
        "comfort": comfort,
        "specs": specs,
    }
