#!/usr/bin/env python3
"""
Scrape ALL dealer/service locations for AGCO brands (Fendt, Massey Ferguson, Valtra)
across Europe and the USA.

AGCO uses a unified POST API at /services/agco/v2/globalDealerLocatorAPIProxy
- Fendt NA: brand="FT", region="NA" via locator.agcocorp.com
- Fendt EME: brand="FE", region="EME" via locator.agcocorp.com
- Massey Ferguson: brand="MF" via www.masseyferguson.com (separate domain avoids rate limit)
- Valtra: Separate API at https://dealer-locator.valtradev.com/Dealer.aspx

The AGCO API caps results at ~200 (NA) or ~100 (EME) per query, so we use a
lat/lon grid approach for Europe to ensure full coverage.
"""

import csv
import json
import os
import sys
import time
import hashlib
import requests

# ============================================================
# Configuration
# ============================================================

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json; charset=UTF-8",
    "Accept": "application/json",
}

# Different domains for different brands to avoid rate limiting
FENDT_API_URL = "https://locator.agcocorp.com/services/agco/v2/globalDealerLocatorAPIProxy"
MF_API_URL = "https://www.masseyferguson.com/services/agco/v2/globalDealerLocatorAPIProxy"

# Delay between API requests (seconds)
REQUEST_DELAY = 1.0
# Extra delay between major sections to avoid rate limiting
SECTION_DELAY = 5.0
# Max retries for 403/rate limit errors
MAX_RETRIES = 3

# ============================================================
# Query Points
# ============================================================

US_QUERY_POINTS = [
    {"lat": 39.8283, "lng": -98.5795, "label": "US-Center", "country": "US"},
    {"lat": 64.2008, "lng": -152.4937, "label": "Alaska", "country": "US"},
    {"lat": 19.8968, "lng": -155.5828, "label": "Hawaii", "country": "US"},
    {"lat": 53.7267, "lng": -106.1699, "label": "Canada-Center", "country": "CA"},
]

# Europe grid: lat 35-71, lon -12 to 40, spacing ~5deg lat x 6deg lon
# 500km radius queries with overlap for complete coverage
EUROPE_GRID = []
for lat in range(35, 72, 5):
    for lng in range(-12, 42, 6):
        EUROPE_GRID.append({
            "lat": float(lat),
            "lng": float(lng),
            "label": f"EU-grid-{lat}-{lng}",
        })

MIDDLE_EAST_GRID = [
    {"lat": 33.0, "lng": 44.0, "label": "ME-Iraq"},
    {"lat": 25.0, "lng": 55.0, "label": "ME-UAE"},
    {"lat": 32.0, "lng": 35.0, "label": "ME-Israel"},
    {"lat": 39.0, "lng": 35.0, "label": "ME-Turkey"},
]

# Valtra country tags for Europe + North America
VALTRA_EUROPE_TAGS = [
    "at", "az", "by", "be", "ba", "bg", "hr", "cy", "cz", "dk",
    "ee", "fr", "ge", "de", "gr", "hu", "is", "ie", "il", "it",
    "lv", "lt", "lu", "md", "nl", "no", "pl", "pt", "ro", "rs",
    "sk", "si", "es", "se", "ch", "tr", "ua", "uk",
]
VALTRA_USA_TAGS = ["us", "ca"]
VALTRA_ALL_TAGS = set(VALTRA_EUROPE_TAGS + VALTRA_USA_TAGS)


# ============================================================
# Helper Functions
# ============================================================

def dealer_fingerprint(dealer):
    """Create a unique key for deduplication based on name + coordinates."""
    name = (dealer.get("dealer_name") or "").strip().lower()
    lat = dealer.get("latitude") or ""
    lng = dealer.get("longitude") or ""
    try:
        lat = f"{float(lat):.4f}"
        lng = f"{float(lng):.4f}"
    except (ValueError, TypeError):
        pass
    raw = f"{name}|{lat}|{lng}"
    return hashlib.md5(raw.encode()).hexdigest()


def query_agco_api(api_url, brand, region, lat, lng, radius,
                   distance_unit="mi", country=None, referer=None):
    """Query the AGCO dealer locator API with retry logic."""
    body = {
        "brand": brand,
        "region": region,
        "radius": str(radius),
        "distance_unit": distance_unit,
        "geocode": {"latitude": lat, "longitude": lng},
        "address_components": {},
    }
    if country:
        body["address_components"]["country"] = country

    headers = dict(HEADERS_BASE)
    if referer:
        headers["Referer"] = referer
        # Also set Origin from referer
        from urllib.parse import urlparse
        parsed = urlparse(referer)
        headers["Origin"] = f"{parsed.scheme}://{parsed.netloc}"

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(api_url, json=body, headers=headers, timeout=30)
            if resp.status_code == 403:
                wait = (attempt + 1) * 10  # 10s, 20s, 30s backoff
                print(f"    Rate limited (403). Waiting {wait}s before retry {attempt+1}/{MAX_RETRIES}...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return []
        except requests.exceptions.HTTPError as e:
            if "403" in str(e):
                wait = (attempt + 1) * 10
                print(f"    Rate limited (403). Waiting {wait}s before retry {attempt+1}/{MAX_RETRIES}...")
                time.sleep(wait)
                continue
            print(f"  ERROR: {e}")
            return []
        except Exception as e:
            print(f"  ERROR: {e}")
            return []

    print(f"  FAILED after {MAX_RETRIES} retries (brand={brand}, region={region}, lat={lat}, lng={lng})")
    return []


def parse_agco_dealer(raw, brand_name, brand_code):
    """Parse a raw AGCO API dealer response into our standardized format."""
    addr = raw.get("address") or {}
    phone = raw.get("phone") or {}
    brands = raw.get("brands") or []
    brand_names = [b.get("name", "") for b in brands]

    services = set()
    for b in brands:
        po = b.get("product_offerings") or {}
        if po.get("salesBusinessType") == "Y":
            services.add("Sales")
        if po.get("serviceBusinessType") == "Y":
            services.add("Service")
        if po.get("partsBusinessType") == "Y":
            services.add("Parts")

    dealer_type = raw.get("dealerLocatorType") or raw.get("dealerLocatorType_translated") or ""

    return {
        "brand": brand_name,
        "brand_code": brand_code,
        "all_brands": "; ".join(brand_names),
        "dealer_name": raw.get("dealer_name") or "",
        "dealer_code": raw.get("dealer_code") or "",
        "dealer_type": dealer_type,
        "address": addr.get("street") or "",
        "city": addr.get("city") or "",
        "state_region": addr.get("state") or "",
        "state_code": addr.get("state_code") or "",
        "country": addr.get("country") or "",
        "country_code": addr.get("country_code") or "",
        "postal_code": addr.get("postal_code") or "",
        "latitude": addr.get("latitude") or "",
        "longitude": addr.get("longitude") or "",
        "phone": phone.get("main") or "",
        "fax": raw.get("fax") or "",
        "email": (raw.get("email") or {}).get("sales") or "",
        "website": raw.get("website") or "",
        "services_offered": "; ".join(sorted(services)),
        "region": raw.get("region") or "",
        "account_id": raw.get("account_id") or "",
    }


def parse_valtra_dealer(raw, country_tag, country_name=""):
    """Parse a raw Valtra API dealer response into our standardized format."""
    phones = raw.get("phones") or []
    emails = raw.get("emails") or []

    dealer_type = "Dealership"
    if raw.get("importer"):
        dealer_type = "Importer"
    if raw.get("q_certified"):
        dealer_type = "Q Certified Dealer"

    return {
        "brand": "Valtra",
        "brand_code": "VA",
        "all_brands": "Valtra",
        "dealer_name": raw.get("name") or "",
        "dealer_code": str(raw.get("id") or ""),
        "dealer_type": dealer_type,
        "address": raw.get("address") or "",
        "city": raw.get("city") or "",
        "state_region": raw.get("area") or "",
        "state_code": "",
        "country": country_name,
        "country_code": country_tag.upper(),
        "postal_code": raw.get("zipcode") or "",
        "latitude": raw.get("lat") or "",
        "longitude": raw.get("lng") or "",
        "phone": phones[0] if phones else "",
        "fax": "",
        "email": emails[0] if emails else "",
        "website": raw.get("url") or "",
        "services_offered": "",
        "region": "EME" if country_tag not in ("us", "ca") else "NA",
        "account_id": "",
    }


def write_csv(dealers, filename):
    """Write dealers to CSV file."""
    if not dealers:
        print(f"  No dealers to write for {filename}")
        return 0

    filepath = os.path.join(OUTPUT_DIR, filename)
    fieldnames = [
        "brand", "brand_code", "all_brands", "dealer_name", "dealer_code",
        "dealer_type", "address", "city", "state_region", "state_code",
        "country", "country_code", "postal_code", "latitude", "longitude",
        "phone", "fax", "email", "website", "services_offered", "region",
        "account_id",
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(dealers)

    print(f"  Wrote {len(dealers)} dealers to {filepath}")
    return len(dealers)


def deduplicate(dealers):
    """Deduplicate dealers by name + coordinates."""
    seen = {}
    unique = []
    for d in dealers:
        fp = dealer_fingerprint(d)
        if fp not in seen:
            seen[fp] = True
            unique.append(d)
    return unique


# ============================================================
# Scraping Functions
# ============================================================

def scrape_agco_brand(brand_name, brand_code_na, brand_code_eme, api_url, referer):
    """
    Generic scraper for an AGCO brand using the unified API.
    Uses separate brand codes for NA vs EME regions.
    """
    print(f"\n{'=' * 60}")
    print(f"SCRAPING {brand_name.upper()} DEALERS")
    print(f"{'=' * 60}")

    all_dealers = []

    # --- North America ---
    print(f"\n--- {brand_name} North America (brand={brand_code_na}, region=NA) ---")
    for point in US_QUERY_POINTS:
        print(f"  Querying {point['label']} (lat={point['lat']}, lng={point['lng']})...")
        results = query_agco_api(
            api_url=api_url,
            brand=brand_code_na, region="NA",
            lat=point["lat"], lng=point["lng"],
            radius=5000, distance_unit="mi",
            country=point.get("country"),
            referer=referer,
        )
        print(f"    Got {len(results)} results")
        for r in results:
            all_dealers.append(parse_agco_dealer(r, brand_name, brand_code_na))
        time.sleep(REQUEST_DELAY)

    time.sleep(SECTION_DELAY)

    # --- Europe & Middle East ---
    print(f"\n--- {brand_name} Europe/Middle East (brand={brand_code_eme}, region=EME) ---")
    grid_points = EUROPE_GRID + MIDDLE_EAST_GRID
    for i, point in enumerate(grid_points):
        print(f"  [{i+1}/{len(grid_points)}] Querying {point['label']} (lat={point['lat']}, lng={point['lng']})...")
        results = query_agco_api(
            api_url=api_url,
            brand=brand_code_eme, region="EME",
            lat=point["lat"], lng=point["lng"],
            radius=500, distance_unit="km",
            referer=referer,
        )
        print(f"    Got {len(results)} results")
        for r in results:
            all_dealers.append(parse_agco_dealer(r, brand_name, brand_code_eme))
        time.sleep(REQUEST_DELAY)

    # Deduplicate
    before = len(all_dealers)
    all_dealers = deduplicate(all_dealers)
    print(f"\n  {brand_name}: {before} total -> {len(all_dealers)} unique dealers")

    return all_dealers


def scrape_fendt():
    """Scrape Fendt dealers using locator.agcocorp.com."""
    dealers = scrape_agco_brand(
        brand_name="Fendt",
        brand_code_na="FT",
        brand_code_eme="FE",
        api_url=FENDT_API_URL,
        referer="https://locator.agcocorp.com/Fendt/en.html",
    )
    write_csv(dealers, "fendt_dealers.csv")
    return dealers


def scrape_massey_ferguson():
    """Scrape MF dealers using www.masseyferguson.com (separate domain)."""
    dealers = scrape_agco_brand(
        brand_name="Massey Ferguson",
        brand_code_na="MF",
        brand_code_eme="MF",
        api_url=MF_API_URL,
        referer="https://www.masseyferguson.com/",
    )
    write_csv(dealers, "massey_ferguson_dealers.csv")
    return dealers


def scrape_valtra():
    """Scrape Valtra dealers from the Valtra-specific API (separate system)."""
    print(f"\n{'=' * 60}")
    print("SCRAPING VALTRA DEALERS")
    print(f"{'=' * 60}")

    all_dealers = []
    valtra_api = "https://dealer-locator.valtradev.com/Dealer.aspx"
    headers = {"User-Agent": HEADERS_BASE["User-Agent"]}

    # Get country list
    print("\n  Fetching country list...")
    try:
        resp = requests.get(valtra_api, headers=headers, timeout=30)
        countries = resp.json()
        print(f"  Found {len(countries)} countries total")
    except Exception as e:
        print(f"  ERROR fetching country list: {e}")
        countries = [{"tag": t, "name": t} for t in sorted(VALTRA_ALL_TAGS)]

    for country in countries:
        tag = country.get("tag", "")
        name = country.get("name", tag)

        if tag not in VALTRA_ALL_TAGS:
            continue

        print(f"  Querying {name} ({tag})...")
        try:
            resp = requests.get(f"{valtra_api}?tag={tag}", headers=headers, timeout=30)
            dealers_raw = resp.json()
            print(f"    Got {len(dealers_raw)} dealers")
            for d in dealers_raw:
                all_dealers.append(parse_valtra_dealer(d, tag, name))
        except Exception as e:
            print(f"    ERROR: {e}")

        time.sleep(0.3)  # Valtra API is lighter, shorter delay

    before = len(all_dealers)
    all_dealers = deduplicate(all_dealers)
    print(f"\n  Valtra: {before} total -> {len(all_dealers)} unique dealers")

    write_csv(all_dealers, "valtra_dealers.csv")
    return all_dealers


# ============================================================
# Main
# ============================================================

def main():
    print("AGCO Dealer Scraper")
    print("Brands: Fendt, Massey Ferguson, Valtra")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Request delay: {REQUEST_DELAY}s | Section delay: {SECTION_DELAY}s")

    # Accept command-line arg to scrape specific brand only
    brands_to_scrape = sys.argv[1:] if len(sys.argv) > 1 else ["fendt", "mf", "valtra"]

    start_time = time.time()

    fendt_dealers = []
    mf_dealers = []
    valtra_dealers = []

    if "fendt" in brands_to_scrape:
        fendt_dealers = scrape_fendt()
        time.sleep(SECTION_DELAY)

    if "mf" in brands_to_scrape:
        mf_dealers = scrape_massey_ferguson()
        time.sleep(SECTION_DELAY)

    if "valtra" in brands_to_scrape:
        valtra_dealers = scrape_valtra()

    # Combined file
    all_dealers = fendt_dealers + mf_dealers + valtra_dealers
    print(f"\n{'=' * 60}")
    print("COMBINED RESULTS")
    print(f"{'=' * 60}")
    print(f"  Fendt: {len(fendt_dealers)} dealers")
    print(f"  Massey Ferguson: {len(mf_dealers)} dealers")
    print(f"  Valtra: {len(valtra_dealers)} dealers")
    print(f"  TOTAL: {len(all_dealers)} dealers")

    if all_dealers:
        write_csv(all_dealers, "agco_dealers_combined.csv")

    # Summary statistics
    print("\n--- Country breakdown ---")
    country_counts = {}
    for d in all_dealers:
        cc = d.get("country_code") or d.get("country") or "Unknown"
        brand = d.get("brand", "Unknown")
        key = f"{cc} ({brand})"
        country_counts[key] = country_counts.get(key, 0) + 1

    for key in sorted(country_counts.keys()):
        print(f"  {key}: {country_counts[key]}")

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()
