import re
import requests
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

    for m in re.findall(r'(/fr/inventaire-occasion/[^\s"\'<>]+?-id\d+)', html, flags=re.IGNORECASE):
        add(m)

    return sorted(out)

def parse_vehicle_detail_simple(session: requests.Session, url: str) -> Dict[str, Any]:
    """
    MVP stable : titre/price/km + photos sm360.
    Plus tard on remplacera par vehicleDetails (brace matching) version KenBot.
    """
    html = fetch_html(session, url)
    soup = BeautifulSoup(html, "html.parser")

    h1 = soup.find("h1")
    title = (h1.get_text(" ", strip=True) if h1 else "").strip() or "Sans titre"

    stock = ""
    vin = ""

    m = re.search(r"stockNumber\s*[:=]\s*['\"]([A-Za-z0-9]+)['\"]", html, re.IGNORECASE)
    if m:
        stock = m.group(1).strip().upper()

    m = re.search(r"\bvin\s*[:=]\s*['\"]([A-HJ-NPR-Z0-9]{11,17})['\"]", html, re.IGNORECASE)
    if m:
        vin = m.group(1).strip().upper()

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

    photos: List[str] = []
    for img in soup.select("img"):
        src = img.get("data-src") or img.get("src")
        if not src:
            continue
        src = src if src.startswith("http") else urljoin(url, src)
        low = src.lower()
        if "img.sm360.ca" in low and "/images/inventory/" in low and "/ir/w75h23/" not in low:
            photos.append(src)

    seen = set()
    uniq = []
    for p in photos:
        if p in seen:
            continue
        seen.add(p)
        uniq.append(p)

    return {
        "url": url,
        "title": title,
        "stock": stock,
        "vin": vin,
        "price": price,
        "mileage": mileage,
        "price_int": _clean_price_int(price) if price else None,
        "km_int": _clean_km_int(mileage) if mileage else None,
        "photos": uniq,
    }
