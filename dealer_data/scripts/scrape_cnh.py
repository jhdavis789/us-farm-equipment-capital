#!/usr/bin/env python3
"""
Scrape ALL dealer/service locations for CNH Industrial brands:
- Case IH (USA + Europe)
- New Holland AG (USA + Europe)
- STEYR (Europe only)

Uses the Sitecore-based dealer locator API endpoints found at:
- caseih.com/apirequest/dealer-locator/
- agriculture.newholland.com/apirequest/dealer-locator/
- steyr-traktoren.com/apirequest/dealer-locator/

API endpoints:
  /apirequest/dealer-locator/get-dealer-by-country?pageId=&language=&country=&countryName=
  /apirequest/dealer-locator/get-dealer-by-geographic-filter?state=&pageId=&language=&country=
  /apirequest/dealer-locator/get-dealer-by-geo-code?latitude=&longitude=&pageId=&language=&country=

Results are capped at 100 per query, so we use state-level and geo-grid queries
to ensure full coverage.
"""

import requests
import csv
import json
import time
import os
import sys
from collections import defaultdict

# ─── Output paths ───────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(SCRIPT_DIR, '..', 'raw')
os.makedirs(RAW_DIR, exist_ok=True)

OUTPUT_CSV = os.path.join(RAW_DIR, 'cnh_dealers.csv')
OUTPUT_JSON = os.path.join(RAW_DIR, 'cnh_dealers.json')
PROGRESS_FILE = os.path.join(RAW_DIR, 'cnh_scrape_progress.json')

# ─── Brand + site configurations ────────────────────────────────────────────
# Each brand/region combo has: base_url (the website domain), pageId, language
CONFIGS = {
    # Case IH USA
    'caseih_us': {
        'brand_name': 'Case IH',
        'base_url': 'https://www.caseih.com',
        'referer': 'https://www.caseih.com/en-us/unitedstates/dealer-locator',
        'page_id': '{12BABBA7-79F2-49A8-B495-DAC335AC856A}',
        'language': 'en',
        'region': 'US',
    },
    # Case IH Europe (en-gb/europe site covers: AL, BA, BG, CH, CY, CZ, EE, FI, GR, HR, HU, IS, LI, LT, LV, MD, MK, NO, PT, RO, RS, SI, SK)
    'caseih_eu': {
        'brand_name': 'Case IH',
        'base_url': 'https://www.caseih.com',
        'referer': 'https://www.caseih.com/en-gb/europe/tools-resources/dealer-locator',
        'page_id': '{13A5BC27-DC93-4013-9233-4C06E9289AB1}',
        'language': 'en',
        'region': 'EU',
    },
    # New Holland USA
    'nh_us': {
        'brand_name': 'New Holland',
        'base_url': 'https://agriculture.newholland.com',
        'referer': 'https://agriculture.newholland.com/en-us/nar/services-and-solutions/find-a-dealer',
        'page_id': '{7465DECF-7E95-4B5E-8271-505BBBB37843}',
        'language': 'en',
        'region': 'US',
    },
}

# ─── US States ──────────────────────────────────────────────────────────────
US_STATES = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
    'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
    'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana',
    'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
    'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
    'New Hampshire', 'New Jersey', 'New Mexico', 'New York',
    'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon',
    'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina',
    'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia',
    'Washington', 'West Virginia', 'Wisconsin', 'Wyoming',
]

# ─── European countries ─────────────────────────────────────────────────────
# Code -> Name mapping
EU_COUNTRIES = {
    'AL': 'Albania', 'AT': 'Austria', 'BA': 'Bosnia and Herzegovina',
    'BE': 'Belgium', 'BG': 'Bulgaria', 'CH': 'Switzerland',
    'CZ': 'Czech Republic', 'DE': 'Germany', 'DK': 'Denmark',
    'EE': 'Estonia', 'ES': 'Spain', 'FI': 'Finland', 'FR': 'France',
    'GB': 'United Kingdom', 'GR': 'Greece', 'HR': 'Croatia',
    'HU': 'Hungary', 'IE': 'Ireland', 'IS': 'Iceland', 'IT': 'Italy',
    'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia',
    'MD': 'Moldova', 'MK': 'Macedonia', 'ME': 'Montenegro',
    'NL': 'Netherlands', 'NO': 'Norway', 'PL': 'Poland', 'PT': 'Portugal',
    'RO': 'Romania', 'RS': 'Serbia', 'SE': 'Sweden', 'SI': 'Slovenia',
    'SK': 'Slovakia', 'UA': 'Ukraine',
}

# Geo-grid points for large countries (lat, lng) to ensure coverage
# Each point will be queried with the geo-code endpoint
# These points are chosen to create overlapping 200km radius circles
COUNTRY_GRID_POINTS = {
    'DE': [  # Germany - ~357k km²
        (54.5, 9.5), (54.5, 12.5), (52.5, 8.0), (52.5, 10.5), (52.5, 13.5),
        (51.0, 7.0), (51.0, 9.5), (51.0, 12.0), (50.0, 7.5), (50.0, 10.0),
        (50.0, 12.5), (49.0, 8.0), (49.0, 10.5), (49.0, 12.5), (48.0, 9.0),
        (48.0, 11.0), (48.0, 13.0), (47.5, 10.0),
    ],
    'FR': [  # France - ~640k km²
        (49.0, 2.5), (48.5, -1.5), (48.0, 5.0), (47.5, -1.0), (47.5, 2.0),
        (47.0, 4.5), (47.0, 6.5), (46.5, -0.5), (46.5, 2.5), (46.0, 5.0),
        (45.5, -1.0), (45.5, 1.5), (45.0, 3.5), (45.0, 6.0), (44.5, -0.5),
        (44.0, 2.0), (44.0, 4.5), (43.5, 1.5), (43.5, 3.5), (43.5, 6.5),
        (43.0, -0.5), (42.5, 3.0), (42.5, 9.0),
    ],
    'IT': [  # Italy - ~301k km²
        (45.5, 8.0), (45.5, 10.5), (45.5, 12.5), (44.5, 8.5), (44.5, 11.0),
        (44.0, 12.5), (43.5, 10.5), (43.0, 12.5), (42.5, 11.5), (42.0, 13.5),
        (41.5, 12.5), (41.0, 15.0), (40.5, 14.0), (40.5, 16.5), (39.5, 16.0),
        (39.0, 9.0), (38.5, 16.5), (37.5, 14.0), (37.0, 15.0),
    ],
    'ES': [  # Spain - ~505k km²
        (43.0, -3.5), (43.0, -8.0), (42.0, -1.0), (42.0, 2.5), (41.5, -4.5),
        (41.0, 0.5), (40.5, -3.5), (40.0, -1.0), (39.5, -4.5), (39.0, -1.0),
        (39.0, -6.5), (38.5, -3.5), (38.0, -0.5), (37.5, -5.5), (37.0, -3.0),
        (36.5, -5.0), (39.5, 3.0), (28.5, -16.0),
    ],
    'GB': [  # UK - ~243k km²
        (57.0, -4.0), (56.0, -3.5), (55.0, -1.5), (54.0, -2.5), (53.5, -1.0),
        (53.0, -3.0), (52.5, 0.5), (52.0, -1.5), (51.5, -2.5), (51.5, 0.0),
        (51.0, 1.0), (50.5, -3.5), (50.5, -1.0),
    ],
    'PL': [  # Poland - ~312k km²
        (54.0, 18.5), (53.5, 15.5), (52.5, 16.5), (52.0, 19.5), (52.0, 21.0),
        (51.5, 17.5), (51.0, 19.5), (51.0, 22.0), (50.5, 16.0), (50.0, 19.0),
        (50.0, 21.5), (49.5, 20.0),
    ],
    'AT': [  # Austria - ~84k km²
        (48.2, 14.0), (48.0, 16.0), (47.5, 10.5), (47.5, 13.0), (47.0, 15.0),
        (47.0, 11.5),
    ],
    'SE': [  # Sweden - ~450k km²
        (67.0, 19.0), (65.0, 17.0), (63.0, 15.0), (61.0, 15.5), (59.5, 15.0),
        (59.0, 17.5), (58.0, 13.5), (57.0, 15.0), (56.0, 13.5), (55.5, 14.5),
    ],
    'RO': [  # Romania - ~238k km²
        (47.5, 23.5), (47.0, 26.0), (46.5, 24.5), (46.0, 22.0), (45.5, 25.5),
        (45.0, 23.0), (44.5, 26.0), (44.0, 24.0),
    ],
    'NL': [(52.0, 5.0), (51.5, 5.5), (53.0, 6.0)],
    'BE': [(50.5, 4.5), (51.0, 3.5)],
    'DK': [(56.0, 10.0), (55.5, 9.0), (57.0, 10.0)],
    'IE': [(53.5, -7.5), (52.0, -8.0)],
    'NO': [(63.0, 10.5), (60.5, 8.0), (59.0, 10.5), (66.0, 14.0), (69.0, 18.0)],
    'HU': [(47.5, 19.0), (46.5, 18.5), (47.0, 20.5)],
    'CZ': [(50.0, 14.5), (49.5, 16.5), (49.0, 18.0)],
    'BG': [(42.7, 25.5), (43.5, 24.5), (42.0, 24.0)],
    # Smaller countries - single center point should suffice
    'LU': [(49.6, 6.1)],
    'LT': [(55.5, 24.0)],
    'LV': [(57.0, 24.5)],
    'EE': [(58.8, 25.0)],
    'SK': [(48.7, 19.5)],
    'SI': [(46.1, 14.8)],
    'HR': [(45.0, 16.0)],
    'RS': [(44.0, 21.0)],
    'AL': [(41.0, 20.0)],
    'BA': [(44.0, 18.0)],
    'MK': [(41.5, 21.5)],
    'MD': [(47.0, 28.5)],
    'UA': [(49.0, 32.0), (48.5, 35.0), (50.5, 30.5), (47.0, 32.5)],
    'IS': [(64.5, -19.0)],
    'ME': [(42.5, 19.3)],
    'PT': [(39.5, -8.0), (41.0, -8.0), (38.0, -8.0)],
    'GR': [(39.5, 22.0), (37.5, 22.5), (38.5, 23.5)],
    'FI': [(61.5, 24.0), (63.5, 26.0), (60.5, 25.0), (65.0, 25.5)],
    'CH': [(47.0, 7.5), (47.0, 8.5), (46.5, 7.0)],
}

# For large US states, use geo-grid to exceed the 100 limit
US_STATE_GRIDS = {
    'Texas': [
        (34.0, -101.5), (33.0, -97.0), (32.0, -100.0), (31.0, -97.0),
        (30.5, -99.5), (30.0, -95.5), (29.5, -98.0), (29.0, -95.5),
        (27.5, -98.5), (26.5, -98.0), (32.5, -95.0), (31.5, -94.0),
    ],
    'Minnesota': [
        (48.0, -95.5), (47.0, -94.5), (46.0, -95.0), (45.5, -93.5),
        (44.5, -94.5), (44.0, -92.5),
    ],
    'Iowa': [
        (43.0, -93.5), (42.5, -95.5), (42.0, -93.0), (41.5, -94.5), (41.5, -91.5),
    ],
    'Illinois': [
        (42.0, -89.0), (41.0, -89.0), (40.0, -89.5), (39.0, -89.5), (38.0, -89.0),
    ],
    'Indiana': [
        (41.5, -86.5), (40.5, -86.0), (39.5, -86.5), (38.5, -86.0),
    ],
    'Ohio': [
        (41.5, -82.5), (40.5, -83.0), (40.0, -81.0), (39.5, -83.5),
    ],
    'Pennsylvania': [
        (42.0, -77.5), (41.0, -76.5), (40.5, -78.5), (40.0, -76.0), (40.0, -80.0),
    ],
    'New York': [
        (43.5, -75.5), (42.5, -76.5), (42.0, -74.0), (41.0, -74.0),
    ],
    'California': [
        (40.5, -122.0), (38.5, -121.5), (37.5, -120.5), (36.5, -119.5),
        (35.5, -119.0), (34.0, -118.0), (33.5, -117.0),
    ],
    'Kansas': [
        (39.5, -98.5), (38.5, -97.5), (38.0, -99.5), (37.5, -97.5),
    ],
    'Nebraska': [
        (42.0, -100.0), (41.5, -97.5), (41.0, -99.5), (40.5, -97.0),
    ],
    'Wisconsin': [
        (45.5, -89.5), (44.5, -89.0), (43.5, -89.5), (43.0, -88.0),
    ],
    'Missouri': [
        (39.5, -93.5), (38.5, -92.5), (37.5, -93.0), (37.0, -90.5),
    ],
    'North Dakota': [
        (48.5, -100.5), (47.5, -99.0), (47.0, -97.5), (46.5, -100.0),
    ],
    'South Dakota': [
        (45.0, -100.0), (44.0, -99.0), (43.5, -97.0), (44.0, -96.5),
    ],
    'Montana': [
        (48.5, -110.0), (47.0, -109.0), (46.0, -110.0), (47.5, -105.0),
    ],
}

# Session setup
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
})


def load_progress():
    """Load progress from checkpoint file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {'completed': [], 'dealers': {}}


def save_progress(progress):
    """Save progress checkpoint."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def api_get(config, endpoint, params, retries=3):
    """Make API request with retries."""
    url = f"{config['base_url']}/apirequest/dealer-locator/{endpoint}"
    headers = {'Referer': config['referer']}

    for attempt in range(retries):
        try:
            time.sleep(1.5)  # Rate limiting
            resp = session.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = int(resp.headers.get('Retry-After', 30))
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif resp.status_code == 403:
                print(f"    403 Forbidden - trying again in {5 * (attempt+1)}s...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"    HTTP {resp.status_code} from {endpoint}")
                if attempt < retries - 1:
                    time.sleep(3 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            print(f"    Request error: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


def get_dealers_by_country(config, country_code, country_name):
    """Get dealers for a country (limited to 100)."""
    params = {
        'pageId': config['page_id'],
        'language': config['language'],
        'country': country_code,
        'countryName': country_name,
    }
    data = api_get(config, 'get-dealer-by-country', params)
    if data:
        return data.get('dealershipResults', [])
    return []


def get_dealers_by_state(config, state_name, country_code='US'):
    """Get dealers for a US state (limited to 100)."""
    params = {
        'state': state_name,
        'pageId': config['page_id'],
        'language': config['language'],
        'country': country_code,
    }
    data = api_get(config, 'get-dealer-by-geographic-filter', params)
    if data:
        return data.get('dealershipResults', [])
    return []


def get_dealers_by_geo(config, lat, lng, country_code):
    """Get dealers near a lat/lng point (limited to 100)."""
    params = {
        'latitude': str(lat),
        'longitude': str(lng),
        'pageId': config['page_id'],
        'language': config['language'],
        'country': country_code,
    }
    data = api_get(config, 'get-dealer-by-geo-code', params)
    if data:
        return data.get('dealershipResults', [])
    return []


def normalize_dealer(raw):
    """Normalize a raw dealer API response into a flat dict."""
    d = raw.get('dealership', {})
    attrs = d.get('dealershipAttributes', {})

    # Extract contract details (services)
    contracts = attrs.get('contractDetails', [])
    services = '; '.join([c.get('codeName', '') for c in contracts if c.get('codeName')])

    # Extract dealer classes
    classes = attrs.get('dealerClasses', [])
    dealer_type = '; '.join([c.get('classDescription', '') for c in classes if c.get('classDescription')])

    return {
        'brand': d.get('brand', ''),
        'dealer_number': d.get('dealerNumber', ''),
        'dealer_name': d.get('dealerName', ''),
        'address': d.get('shippingAddress1', ''),
        'city': d.get('shippingCity', ''),
        'state_region': d.get('shippingStateProv', ''),
        'country': d.get('country', ''),
        'country_code': d.get('countryCode', ''),
        'postal_code': d.get('shippingZip', ''),
        'latitude': d.get('latitude', ''),
        'longitude': d.get('longitude', ''),
        'phone': d.get('shippingPhone', ''),
        'fax': d.get('shippingFax', ''),
        'email': d.get('dealerEmail', ''),
        'website': d.get('dealerWebsite', ''),
        'dealer_type': dealer_type,
        'services_offered': services,
        'super_region': d.get('superRegion', ''),
        'region': d.get('region', ''),
        'sap_number': d.get('cnhPrimarySAPNumber', ''),
        'distance': raw.get('distance', ''),
    }


def merge_dealers(existing, new_results):
    """Merge new dealer results into existing dict, keyed by dealer_number."""
    for raw in new_results:
        d = normalize_dealer(raw)
        key = d['dealer_number']
        if key and key not in existing:
            existing[key] = d
    return existing


def scrape_us_dealers(config_key, progress):
    """Scrape all US dealers for a brand config."""
    config = CONFIGS[config_key]
    brand = config['brand_name']
    dealers = {}

    # Reconstruct dealers from progress
    for key, d in progress.get('dealers', {}).items():
        if d.get('brand') == brand and d.get('country_code') == 'US':
            dealers[key] = d

    print(f"\n{'='*60}")
    print(f"Scraping US dealers: {brand}")
    print(f"{'='*60}")

    for state in US_STATES:
        task_key = f"{config_key}_state_{state}"
        if task_key in progress.get('completed', []):
            continue

        print(f"  {state}...", end=' ', flush=True)
        results = get_dealers_by_state(config, state)
        count_before = len(dealers)
        dealers = merge_dealers(dealers, results)
        count_new = len(dealers) - count_before
        print(f"{len(results)} returned, {count_new} new (total: {len(dealers)})")

        # If we hit 100, use geo-grid for better coverage
        if len(results) >= 100 and state in US_STATE_GRIDS:
            print(f"    Hit 100 limit, using geo-grid for {state}...")
            for lat, lng in US_STATE_GRIDS[state]:
                geo_results = get_dealers_by_geo(config, lat, lng, 'US')
                count_before = len(dealers)
                # Only merge dealers from this state
                state_results = [r for r in geo_results
                                 if r.get('dealership', {}).get('shippingStateProv', '').upper() == state[:2].upper()
                                 or r.get('dealership', {}).get('country', '') == 'United States']
                dealers = merge_dealers(dealers, state_results)
                count_new = len(dealers) - count_before
                if count_new > 0:
                    print(f"      Geo ({lat:.1f}, {lng:.1f}): +{count_new}")

        progress.setdefault('completed', []).append(task_key)
        # Update progress with all dealers
        for k, v in dealers.items():
            progress.setdefault('dealers', {})[k] = v
        save_progress(progress)

    # Final count
    us_dealers = {k: v for k, v in dealers.items() if v.get('country_code') == 'US'}
    print(f"\n  Total {brand} US dealers: {len(us_dealers)}")
    return dealers


def scrape_eu_dealers(config_key, progress):
    """Scrape all European dealers for a brand config."""
    config = CONFIGS[config_key]
    brand = config['brand_name']
    dealers = {}

    # Reconstruct from progress
    for key, d in progress.get('dealers', {}).items():
        if d.get('brand') == brand and d.get('country_code') != 'US':
            dealers[key] = d

    print(f"\n{'='*60}")
    print(f"Scraping EU dealers: {brand}")
    print(f"{'='*60}")

    for code, name in sorted(EU_COUNTRIES.items()):
        task_key = f"{config_key}_country_{code}"
        if task_key in progress.get('completed', []):
            continue

        print(f"  {name} ({code})...", end=' ', flush=True)

        # First try country endpoint
        results = get_dealers_by_country(config, code, name)
        count_before = len(dealers)
        dealers = merge_dealers(dealers, results)
        count_new = len(dealers) - count_before
        print(f"{len(results)} returned, {count_new} new", end='')

        # If we hit 100, use geo-grid
        if len(results) >= 100 and code in COUNTRY_GRID_POINTS:
            print(f" -> using geo-grid...", end='')
            for lat, lng in COUNTRY_GRID_POINTS[code]:
                geo_results = get_dealers_by_geo(config, lat, lng, code)
                cb = len(dealers)
                dealers = merge_dealers(dealers, geo_results)
                cn = len(dealers) - cb
                if cn > 0:
                    print(f" +{cn}", end='')
            print(f" (total: {len(dealers)})")
        elif len(results) == 0:
            # Try geo-grid for countries that might not work with country endpoint
            if code in COUNTRY_GRID_POINTS:
                print(f" -> trying geo-grid...", end='')
                for lat, lng in COUNTRY_GRID_POINTS[code]:
                    geo_results = get_dealers_by_geo(config, lat, lng, code)
                    cb = len(dealers)
                    dealers = merge_dealers(dealers, geo_results)
                    cn = len(dealers) - cb
                    if cn > 0:
                        print(f" +{cn}", end='')
                print()
            else:
                print()
        else:
            print(f" (total: {len(dealers)})")

        progress.setdefault('completed', []).append(task_key)
        for k, v in dealers.items():
            progress.setdefault('dealers', {})[k] = v
        save_progress(progress)

    print(f"\n  Total {brand} EU dealers: {len(dealers)}")
    return dealers


def try_discover_nh_eu_config():
    """Try to discover New Holland EU and STEYR dealer locator configs."""
    discovered = {}

    # New Holland EU - try different regional pages
    nh_eu_urls = [
        ('https://agriculture.newholland.com/en-gb/eur/services-and-solutions/find-a-dealer',
         'agriculture.newholland.com'),
        ('https://agriculture.newholland.com/en/europe/services-and-solutions/find-a-dealer',
         'agriculture.newholland.com'),
        ('https://agriculture.newholland.com/de-de/eur/services-and-solutions/find-a-dealer',
         'agriculture.newholland.com'),
    ]

    print("\n--- Discovering New Holland EU pageId ---")
    for url, domain in nh_eu_urls:
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                import re
                decoded = resp.text.encode('raw_unicode_escape').decode('unicode_escape', errors='ignore')
                matches = re.findall(r'"pageId"\s*:\s*"([^"]+)"', decoded)
                if matches:
                    page_id = matches[0]
                    print(f"  Found NH EU pageId: {page_id} from {url}")
                    discovered['nh_eu'] = {
                        'brand_name': 'New Holland',
                        'base_url': f'https://{domain}',
                        'referer': url,
                        'page_id': page_id,
                        'language': 'en',
                        'region': 'EU',
                    }
                    break
        except Exception as e:
            print(f"  Error probing {url}: {e}")

    # STEYR
    steyr_urls = [
        ('https://www.steyr-traktoren.com/en/agriculture/purchase-and-offer/dealer-locator',
         'www.steyr-traktoren.com'),
    ]

    print("\n--- Discovering STEYR pageId ---")
    for url, domain in steyr_urls:
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 200:
                import re
                decoded = resp.text.encode('raw_unicode_escape').decode('unicode_escape', errors='ignore')
                matches = re.findall(r'"pageId"\s*:\s*"([^"]+)"', decoded)
                if matches:
                    page_id = matches[0]
                    print(f"  Found STEYR pageId: {page_id} from {url}")
                    discovered['steyr_eu'] = {
                        'brand_name': 'STEYR',
                        'base_url': f'https://{domain}',
                        'referer': url,
                        'page_id': page_id,
                        'language': 'en',
                        'region': 'EU',
                    }
                    break
        except Exception as e:
            print(f"  Error probing {url}: {e}")

    return discovered


def try_cross_brand_eu_queries(progress):
    """
    Try to query the CaseIH EU endpoint for New Holland and STEYR dealers.
    CNH brands share the same backend, so the same API might return
    different brand results depending on pageId.
    """
    dealers = {}

    # If we couldn't discover specific NH/STEYR EU configs,
    # we can try using the CaseIH EU endpoint with geo-queries
    # and see if it returns NH/STEYR dealers too
    print("\n--- Trying cross-brand geo queries via CaseIH EU endpoint ---")
    config = CONFIGS['caseih_eu']

    # Test with a German city - see if we get NH dealers
    test = get_dealers_by_geo(config, 51.0, 10.0, 'DE')
    if test:
        brands_found = set()
        for r in test:
            b = r.get('dealership', {}).get('brand', '')
            brands_found.add(b)
        print(f"  Brands found via CaseIH EU endpoint: {brands_found}")

    return dealers


def save_to_csv(dealers_dict, filepath):
    """Save dealers dict to CSV."""
    dealers = list(dealers_dict.values())
    if not dealers:
        print("No dealers to save!")
        return

    fieldnames = [
        'brand', 'dealer_number', 'dealer_name', 'address', 'city',
        'state_region', 'country', 'country_code', 'postal_code',
        'latitude', 'longitude', 'phone', 'fax', 'email', 'website',
        'dealer_type', 'services_offered', 'super_region', 'region',
        'sap_number',
    ]

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for d in sorted(dealers, key=lambda x: (x.get('brand', ''), x.get('country_code', ''), x.get('state_region', ''), x.get('city', ''))):
            writer.writerow(d)

    print(f"Saved {len(dealers)} dealers to {filepath}")


def save_to_json(dealers_dict, filepath):
    """Save dealers dict to JSON."""
    dealers = list(dealers_dict.values())
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(dealers, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(dealers)} dealers to {filepath}")


def print_summary(dealers_dict):
    """Print summary statistics."""
    dealers = list(dealers_dict.values())

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total dealers: {len(dealers)}")

    by_brand = defaultdict(int)
    by_country = defaultdict(int)
    by_brand_region = defaultdict(int)
    with_coords = 0
    with_phone = 0
    with_email = 0

    for d in dealers:
        by_brand[d.get('brand', 'Unknown')] += 1
        cc = d.get('country_code', 'Unknown')
        by_country[cc] += 1
        sr = 'US' if cc == 'US' else 'EU'
        by_brand_region[f"{d.get('brand', '?')} ({sr})"] += 1
        if d.get('latitude') and d.get('longitude'):
            with_coords += 1
        if d.get('phone'):
            with_phone += 1
        if d.get('email'):
            with_email += 1

    print(f"\nBy Brand:")
    for brand, count in sorted(by_brand.items()):
        print(f"  {brand}: {count}")

    print(f"\nBy Brand+Region:")
    for br, count in sorted(by_brand_region.items()):
        print(f"  {br}: {count}")

    print(f"\nBy Country (top 20):")
    for country, count in sorted(by_country.items(), key=lambda x: -x[1])[:20]:
        print(f"  {country}: {count}")

    total = max(1, len(dealers))
    print(f"\nData completeness:")
    print(f"  With coordinates: {with_coords}/{len(dealers)} ({100*with_coords/total:.1f}%)")
    print(f"  With phone: {with_phone}/{len(dealers)} ({100*with_phone/total:.1f}%)")
    print(f"  With email: {with_email}/{len(dealers)} ({100*with_email/total:.1f}%)")


def main():
    print("=" * 70)
    print("CNH Industrial Dealer Scraper")
    print("Brands: Case IH, New Holland AG, STEYR")
    print("Regions: USA + Europe")
    print("=" * 70)

    progress = load_progress()
    all_dealers = progress.get('dealers', {})

    # ─── Case IH USA ────────────────────────────────────────────────────
    caseih_us = scrape_us_dealers('caseih_us', progress)
    all_dealers.update(caseih_us)

    # ─── New Holland USA ────────────────────────────────────────────────
    nh_us = scrape_us_dealers('nh_us', progress)
    all_dealers.update(nh_us)

    # ─── Case IH Europe ─────────────────────────────────────────────────
    caseih_eu = scrape_eu_dealers('caseih_eu', progress)
    all_dealers.update(caseih_eu)

    # ─── Discover New Holland EU + STEYR configs ────────────────────────
    discovered = try_discover_nh_eu_config()

    if 'nh_eu' in discovered:
        CONFIGS['nh_eu'] = discovered['nh_eu']
        nh_eu = scrape_eu_dealers('nh_eu', progress)
        all_dealers.update(nh_eu)
    else:
        print("\n[WARN] Could not discover New Holland EU pageId. Trying CaseIH EU endpoint...")
        # The CaseIH EU endpoint might return NH dealers too
        try_cross_brand_eu_queries(progress)

    if 'steyr_eu' in discovered:
        CONFIGS['steyr_eu'] = discovered['steyr_eu']
        steyr_eu = scrape_eu_dealers('steyr_eu', progress)
        all_dealers.update(steyr_eu)
    else:
        print("\n[WARN] Could not discover STEYR EU pageId.")

    # ─── Save results ───────────────────────────────────────────────────
    save_to_csv(all_dealers, OUTPUT_CSV)
    save_to_json(all_dealers, OUTPUT_JSON)
    print_summary(all_dealers)

    print(f"\nOutput files:")
    print(f"  CSV:  {OUTPUT_CSV}")
    print(f"  JSON: {OUTPUT_JSON}")


if __name__ == '__main__':
    main()
