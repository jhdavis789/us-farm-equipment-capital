#!/usr/bin/env python3
"""
Scrape ALL John Deere dealer/service locations across the USA and Europe.

Uses John Deere's Global Dealer Locator API:
  POST https://dealerlocatorapi.deere.com/api/gdl-service/gdl/dealersByIndustry

The API returns max 15 results per query, so we use a grid of lat/lon points
with spacing of ~0.8 degrees (~55 miles) to ensure overlapping coverage.

Strategy:
  - Phase 1: Query Agriculture (industry 7) across all grid points — this covers
    the vast majority of dealers since most JD dealers handle ag equipment
  - Phase 2: Query Lawn & Garden (6), Construction (2), Forestry (4) to catch
    dealers that only serve those industries
  - Deduplicate by locationId
  - Use ThreadPoolExecutor for concurrent requests (10 workers)
  - Save checkpoints after each country/region
"""

import requests
import json
import csv
import time
import os
import sys
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL = "https://dealerlocatorapi.deere.com/api/gdl-service"
ENDPOINT = f"{BASE_URL}/gdl/dealersByIndustry"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Origin': 'https://dealerlocator.deere.com',
    'Referer': 'https://dealerlocator.deere.com/',
}

# Industries to query — Agriculture first (most comprehensive), then others
INDUSTRIES = [
    (7, 'Agriculture'),
    (6, 'Lawn & Garden'),
    (2, 'Construction'),
    (4, 'Forestry'),
]

# Concurrency settings
MAX_WORKERS = 10
MAX_RETRIES = 3
RETRY_DELAY = 1.5

# ─── Grid definitions ────────────────────────────────────────────────────────

GRID_SPACING = 0.75  # degrees (~52 miles / 83 km)

# Each entry: (name, lat_min, lat_max, lon_min, lon_max, country_code, locale)
ALL_GRIDS = [
    # === USA ===
    ("US_contiguous", 24.0, 50.0, -125.0, -66.0, "US", "en_US"),
    ("US_alaska", 57.0, 66.0, -165.0, -133.0, "US", "en_US"),  # tighter bounds; few dealers in far north
    ("US_hawaii", 19.0, 22.5, -160.5, -154.5, "US", "en_US"),

    # === Western Europe ===
    ("UK", 49.5, 59.0, -8.0, 2.0, "GB", "en_GB"),
    ("Ireland", 51.3, 55.5, -10.5, -5.5, "IE", "en_IE"),
    ("France", 42.0, 51.2, -5.0, 8.3, "FR", "fr_FR"),
    ("Germany", 47.2, 55.2, 5.8, 15.2, "DE", "de_DE"),
    ("Netherlands", 50.7, 53.6, 3.3, 7.2, "NL", "nl_NL"),
    ("Belgium", 49.5, 51.5, 2.5, 6.4, "BE", "fr_BE"),
    ("Luxembourg", 49.4, 50.2, 5.7, 6.5, "LU", "fr_LU"),
    ("Austria", 46.4, 49.0, 9.5, 17.2, "AT", "de_AT"),
    ("Switzerland", 45.8, 47.8, 5.9, 10.5, "CH", "de_CH"),

    # === Northern Europe ===
    ("Denmark", 54.5, 57.8, 8.0, 15.2, "DK", "da_DK"),
    ("Sweden", 55.3, 69.0, 11.0, 24.2, "SE", "sv_SE"),
    ("Norway", 58.0, 71.0, 4.5, 31.0, "NO", "no_NO"),
    ("Finland", 59.8, 70.0, 20.5, 31.5, "FI", "fi_FI"),
    ("Estonia", 57.5, 59.7, 21.8, 28.2, "EE", "et_EE"),
    ("Latvia", 55.7, 58.1, 21.0, 28.2, "LV", "lv_LV"),
    ("Lithuania", 53.9, 56.5, 21.0, 26.8, "LT", "lt_LT"),

    # === Southern Europe ===
    ("Spain", 36.0, 43.8, -9.3, 4.3, "ES", "es_ES"),
    ("Portugal", 37.0, 42.2, -9.5, -6.2, "PT", "pt_PT"),
    ("Italy", 36.6, 47.1, 6.6, 18.5, "IT", "it_IT"),
    ("Greece", 34.8, 41.8, 19.4, 29.6, "GR", "el_GR"),
    ("Croatia", 42.4, 46.6, 13.5, 19.4, "HR", "hr_HR"),
    ("Slovenia", 45.4, 46.9, 13.4, 16.6, "SI", "sl_SI"),
    ("Serbia", 42.2, 46.2, 18.8, 23.0, "RS", "sr_RS"),

    # === Eastern Europe ===
    ("Poland", 49.0, 54.8, 14.1, 24.2, "PL", "pl_PL"),
    ("Czech_Republic", 48.5, 51.1, 12.1, 18.9, "CZ", "cs_CZ"),
    ("Slovakia", 47.7, 49.6, 16.8, 22.6, "SK", "sk_SK"),
    ("Hungary", 45.7, 48.6, 16.2, 22.9, "HU", "hu_HU"),
    ("Romania", 43.6, 48.3, 20.2, 30.0, "RO", "ro_RO"),
    ("Bulgaria", 41.2, 44.2, 22.4, 28.6, "BG", "bg_BG"),
    ("Ukraine", 44.3, 52.4, 22.1, 40.2, "UA", "uk_UA"),
    ("Georgia", 41.0, 43.6, 40.0, 46.7, "GE", "ka_GE"),
]


# ─── Helper Functions ────────────────────────────────────────────────────────

def generate_grid_points(lat_min, lat_max, lon_min, lon_max, spacing):
    """Generate a grid of lat/lon points covering the bounding box."""
    points = []
    lat = lat_min
    while lat <= lat_max + 0.01:
        lon = lon_min
        while lon <= lon_max + 0.01:
            points.append((round(lat, 4), round(lon, 4)))
            lon += spacing
        lat += spacing
    return points


def make_session():
    """Create a new requests session with headers."""
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


# Thread-local storage for sessions
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, 'session'):
        thread_local.session = make_session()
    return thread_local.session


def query_dealers(lat, lon, country_code, locale, industry_id):
    """Query the API for dealers near a point. Thread-safe."""
    session = get_session()
    payload = {
        "industryId": industry_id,
        "countryCode": country_code,
        "brand": "johndeere",
        "equipmentName": "",
        "equipmentId": 0,
        "locale": locale,
        "allIndustriesSelected": False,
        "latitude": lat,
        "longitude": lon,
        "radius": "100 MI",
        "showNonDeereDealer": False
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(ENDPOINT, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('location', [])
            elif resp.status_code == 429:
                time.sleep(RETRY_DELAY * (attempt + 2))
            elif resp.status_code == 404:
                # "Data not found" — no dealers, not an error
                return []
            else:
                if attempt == MAX_RETRIES - 1:
                    return []
                time.sleep(RETRY_DELAY)
        except requests.exceptions.Timeout:
            time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception:
            time.sleep(RETRY_DELAY)
    return []


def parse_dealer(location, country_code):
    """Parse a dealer location into a flat dict."""
    address_parts = location.get('formattedAddress', [])

    street = address_parts[0].strip() if len(address_parts) > 0 else ''
    city_state_zip = address_parts[-1].strip() if len(address_parts) > 1 else ''

    city = ''
    state_region = ''
    postal_code = ''

    if city_state_zip:
        parts = city_state_zip.strip()

        if country_code == 'US':
            tokens = parts.split()
            if len(tokens) >= 3:
                potential_zip = tokens[-1]
                if potential_zip.replace('-', '').isdigit():
                    postal_code = potential_zip
                    state_region = tokens[-2]
                    city = ' '.join(tokens[:-2])
                else:
                    city = parts
            elif len(tokens) == 2:
                state_region = tokens[-1] if len(tokens[-1]) == 2 else ''
                city = tokens[0]
            else:
                city = parts
        elif country_code == 'GB' or country_code == 'IE':
            # UK format: "City POSTCODE" where postcode has a space (e.g., "Alton GU34 3HD")
            # Try to detect UK postcode pattern
            tokens = parts.split()
            if len(tokens) >= 2:
                # UK postcodes: last 2 tokens often form the outcode/incode
                if len(tokens[-1]) == 3 and tokens[-1][0].isdigit():
                    postal_code = tokens[-2] + ' ' + tokens[-1]
                    city = ' '.join(tokens[:-2])
                else:
                    city = parts
            else:
                city = parts
        else:
            # European: typically "PostalCode City" or "City PostalCode"
            tokens = parts.split()
            if tokens:
                # Check for leading postal code (common in DE, FR, IT, ES, etc.)
                first = tokens[0]
                if first.replace('-', '').replace(' ', '').isdigit() and len(first) >= 4:
                    postal_code = first
                    city = ' '.join(tokens[1:])
                elif len(tokens) >= 2 and tokens[-1].replace('-', '').isdigit() and len(tokens[-1]) >= 4:
                    postal_code = tokens[-1]
                    city = ' '.join(tokens[:-1])
                else:
                    city = parts

    contact = location.get('contactDetail', {}) or {}

    services = []
    if location.get('sellIndicator') == 'true':
        services.append('Sales')
    if location.get('partIndicator') == 'true':
        services.append('Parts')
    if location.get('serviceIndicator') == 'true':
        services.append('Service')

    return {
        'brand': 'John Deere',
        'dealer_name': location.get('locationName', '').strip(),
        'address': street,
        'city': city.strip().rstrip(','),
        'state_region': state_region.strip(),
        'country': country_code,
        'postal_code': postal_code.strip(),
        'latitude': location.get('latitude', ''),
        'longitude': location.get('longitude', ''),
        'phone': (contact.get('phone') or '').strip(),
        'dealer_type': '',  # Will be populated per-industry
        'services_offered': '; '.join(services),
        'location_id': location.get('locationId', ''),
        'fax': (contact.get('fax') or '').strip(),
        'email': (contact.get('email') or '').strip(),
        'website': (contact.get('website') or '').strip(),
    }


def save_csv(all_dealers, filepath):
    """Save results to CSV."""
    if not all_dealers:
        return

    fieldnames = ['brand', 'dealer_name', 'address', 'city', 'state_region', 'country',
                  'postal_code', 'latitude', 'longitude', 'phone', 'dealer_type',
                  'services_offered', 'location_id', 'fax', 'email', 'website']

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for dealer in all_dealers.values():
            writer.writerow(dealer)


def scrape_grid(grid_def, industry_id, industry_name, all_dealers, lock):
    """Scrape a single grid region for a single industry using thread pool."""
    grid_name, lat_min, lat_max, lon_min, lon_max, country_code, locale = grid_def
    points = generate_grid_points(lat_min, lat_max, lon_min, lon_max, GRID_SPACING)

    new_count = 0
    total_points = len(points)

    def process_point(args):
        lat, lon = args
        return query_dealers(lat, lon, country_code, locale, industry_id)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_point, p): p for p in points}
        completed = 0

        for future in as_completed(futures):
            completed += 1
            try:
                dealers = future.result()
                if dealers:
                    with lock:
                        for dlr in dealers:
                            loc_id = dlr.get('locationId', '')
                            if loc_id and loc_id not in all_dealers:
                                parsed = parse_dealer(dlr, country_code)
                                # Track which industries this dealer serves
                                parsed['dealer_type'] = industry_name
                                all_dealers[loc_id] = parsed
                                new_count += 1
                            elif loc_id and loc_id in all_dealers:
                                # Append industry type if new
                                existing = all_dealers[loc_id]
                                if industry_name not in existing['dealer_type']:
                                    existing['dealer_type'] += f'; {industry_name}'
            except Exception as e:
                pass  # Already handled in query_dealers

            if completed % 200 == 0:
                with lock:
                    print(f"    {grid_name}/{industry_name}: {completed}/{total_points} "
                          f"({new_count} new, total={len(all_dealers)})")

    return new_count


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    all_dealers = OrderedDict()
    lock = threading.Lock()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(os.path.dirname(script_dir), 'raw')
    output_file = os.path.join(output_dir, 'deere_dealers.csv')
    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()

    # Separate US and Europe grids
    us_grids = [g for g in ALL_GRIDS if g[5] == "US"]
    eu_grids = [g for g in ALL_GRIDS if g[5] != "US"]

    # ─── Phase 1: Agriculture across everything ──────────────────────────
    print("=" * 70)
    print("PHASE 1: Agriculture (industry 7) — broadest coverage")
    print("=" * 70)

    for grid_def in ALL_GRIDS:
        new = scrape_grid(grid_def, 7, 'Agriculture', all_dealers, lock)
        print(f"  {grid_def[0]:25s}: +{new:4d} new  (total: {len(all_dealers)})")

    phase1_count = len(all_dealers)
    elapsed = time.time() - start_time
    print(f"\nPhase 1 complete: {phase1_count} dealers in {elapsed:.0f}s")
    save_csv(all_dealers, output_file)
    print(f"Saved checkpoint.")

    # ─── Phase 2: Other industries to catch specialized dealers ──────────
    print("\n" + "=" * 70)
    print("PHASE 2: Lawn & Garden (6), Construction (2), Forestry (4)")
    print("=" * 70)

    for industry_id, industry_name in [(6, 'Lawn & Garden'), (2, 'Construction'), (4, 'Forestry')]:
        print(f"\n--- {industry_name} (industry {industry_id}) ---")
        phase_start = len(all_dealers)
        for grid_def in ALL_GRIDS:
            new = scrape_grid(grid_def, industry_id, industry_name, all_dealers, lock)
            if new > 0:
                print(f"  {grid_def[0]:25s}: +{new:4d} new  (total: {len(all_dealers)})")
        phase_new = len(all_dealers) - phase_start
        print(f"  {industry_name} added: {phase_new} new dealers")
        save_csv(all_dealers, output_file)

    # ─── Final Summary ───────────────────────────────────────────────────
    total_time = time.time() - start_time
    print(f"\n{'=' * 70}")
    print(f"COMPLETE: {len(all_dealers)} total unique dealers in {total_time:.0f}s ({total_time/60:.1f} min)")
    print(f"  After Phase 1 (Agriculture): {phase1_count}")
    print(f"  Added by other industries:   {len(all_dealers) - phase1_count}")
    print(f"  Saved to: {output_file}")

    # By country
    country_counts = {}
    for d in all_dealers.values():
        c = d['country']
        country_counts[c] = country_counts.get(c, 0) + 1
    print(f"\nDealers by country:")
    for c in sorted(country_counts.keys(), key=lambda x: -country_counts[x]):
        print(f"  {c}: {country_counts[c]}")

    # By region
    us_total = country_counts.get('US', 0)
    eu_total = sum(v for k, v in country_counts.items() if k != 'US')
    print(f"\nUSA total:    {us_total}")
    print(f"Europe total: {eu_total}")


if __name__ == '__main__':
    main()
