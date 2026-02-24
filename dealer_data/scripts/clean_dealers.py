#!/usr/bin/env python3
"""Clean the unified tractor dealer dataset based on quality analysis findings."""

import csv
import os
import sys
from collections import Counter

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
PROCESSED_DIR = os.path.join(BASE_DIR, 'processed')
INPUT_PATH = os.path.join(PROCESSED_DIR, 'all_dealers.csv')
OUTPUT_PATH = os.path.join(PROCESSED_DIR, 'all_dealers_cleaned.csv')

COLUMNS = [
    'brand', 'parent_company', 'dealer_name', 'address', 'city',
    'state_region', 'country_code', 'country_name', 'postal_code',
    'latitude', 'longitude', 'phone', 'email', 'website',
    'dealer_type', 'services_offered',
]

# Country code -> name
CC_TO_NAME = {
    'US': 'United States', 'CA': 'Canada', 'MX': 'Mexico',
    'GB': 'United Kingdom', 'DE': 'Germany', 'FR': 'France',
    'IT': 'Italy', 'ES': 'Spain', 'PT': 'Portugal',
    'NL': 'Netherlands', 'BE': 'Belgium', 'LU': 'Luxembourg',
    'AT': 'Austria', 'CH': 'Switzerland', 'PL': 'Poland',
    'CZ': 'Czech Republic', 'SK': 'Slovakia', 'HU': 'Hungary',
    'RO': 'Romania', 'BG': 'Bulgaria', 'HR': 'Croatia',
    'SI': 'Slovenia', 'RS': 'Serbia', 'BA': 'Bosnia and Herzegovina',
    'ME': 'Montenegro', 'MK': 'North Macedonia', 'AL': 'Albania',
    'GR': 'Greece', 'TR': 'Turkey', 'CY': 'Cyprus',
    'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark',
    'FI': 'Finland', 'IS': 'Iceland', 'EE': 'Estonia',
    'LV': 'Latvia', 'LT': 'Lithuania',
    'IE': 'Ireland', 'UA': 'Ukraine', 'BY': 'Belarus',
    'RU': 'Russia', 'MD': 'Moldova',
    'AU': 'Australia', 'NZ': 'New Zealand',
    'BR': 'Brazil', 'AR': 'Argentina', 'CL': 'Chile',
    'CO': 'Colombia', 'PE': 'Peru', 'UY': 'Uruguay',
    'ZA': 'South Africa', 'KE': 'Kenya', 'NG': 'Nigeria',
    'JP': 'Japan', 'KR': 'South Korea', 'CN': 'China',
    'IN': 'India', 'TH': 'Thailand', 'ID': 'Indonesia',
    'MY': 'Malaysia', 'PH': 'Philippines', 'VN': 'Vietnam',
    'IL': 'Israel', 'SA': 'Saudi Arabia', 'AE': 'United Arab Emirates',
    'EG': 'Egypt', 'MA': 'Morocco', 'TN': 'Tunisia',
    'DZ': 'Algeria', 'PK': 'Pakistan', 'BD': 'Bangladesh',
    'LK': 'Sri Lanka', 'KZ': 'Kazakhstan', 'UZ': 'Uzbekistan',
    'GE': 'Georgia', 'AM': 'Armenia', 'AZ': 'Azerbaijan',
    'PA': 'Panama', 'CR': 'Costa Rica', 'GT': 'Guatemala',
    'EC': 'Ecuador', 'VE': 'Venezuela', 'BO': 'Bolivia',
    'PY': 'Paraguay', 'DO': 'Dominican Republic', 'JM': 'Jamaica',
    'TT': 'Trinidad and Tobago', 'HN': 'Honduras', 'SV': 'El Salvador',
    'NI': 'Nicaragua', 'CU': 'Cuba', 'HT': 'Haiti',
    'MT': 'Malta', 'XK': 'Kosovo',
    'NA': 'Namibia', 'BW': 'Botswana', 'ZW': 'Zimbabwe',
    'MZ': 'Mozambique', 'UG': 'Uganda', 'TZ': 'Tanzania',
    'ZM': 'Zambia', 'SD': 'Sudan', 'SN': 'Senegal',
    'CM': 'Cameroon', 'TD': 'Chad', 'AO': 'Angola',
    'BJ': 'Benin', 'CI': 'Ivory Coast', 'GH': 'Ghana',
    'LB': 'Lebanon', 'JO': 'Jordan', 'IQ': 'Iraq',
    'QA': 'Qatar', 'OM': 'Oman', 'BH': 'Bahrain',
    'KW': 'Kuwait', 'YE': 'Yemen',
    'SG': 'Singapore', 'TW': 'Taiwan', 'KH': 'Cambodia',
    'BB': 'Barbados', 'SR': 'Suriname',
    'LI': 'Liechtenstein', 'GG': 'Guernsey', 'JE': 'Jersey',
    'RE': 'Reunion', 'MM': 'Myanmar', 'KG': 'Kyrgyzstan',
    'ET': 'Ethiopia', 'NC': 'New Caledonia', 'GN': 'Guinea',
    'MR': 'Mauritania',
}

# Country bounding boxes for coordinate-based inference
COUNTRY_BOUNDS = {
    'US': {'lat': (24.0, 72.0), 'lng': (-180.0, -65.0)},
    'CA': {'lat': (41.0, 84.0), 'lng': (-141.0, -52.0)},
    'DE': {'lat': (47.0, 55.5), 'lng': (5.5, 15.5)},
    'AT': {'lat': (46.3, 49.1), 'lng': (9.5, 17.2)},
    'FR': {'lat': (41.0, 51.5), 'lng': (-5.5, 10.0)},
    'IT': {'lat': (36.0, 47.5), 'lng': (6.5, 18.6)},
    'ES': {'lat': (27.5, 44.0), 'lng': (-18.5, 4.5)},
    'GB': {'lat': (49.5, 61.0), 'lng': (-8.5, 2.0)},
    'CH': {'lat': (45.8, 47.9), 'lng': (5.9, 10.5)},
    'NL': {'lat': (50.7, 53.6), 'lng': (3.3, 7.3)},
    'BE': {'lat': (49.5, 51.6), 'lng': (2.5, 6.5)},
    'PL': {'lat': (49.0, 55.0), 'lng': (14.0, 24.2)},
    'CZ': {'lat': (48.5, 51.1), 'lng': (12.1, 18.9)},
    'SK': {'lat': (47.7, 49.7), 'lng': (16.8, 22.6)},
    'HU': {'lat': (45.7, 48.6), 'lng': (16.1, 22.9)},
    'RO': {'lat': (43.5, 48.3), 'lng': (20.2, 30.0)},
    'BG': {'lat': (41.2, 44.3), 'lng': (22.3, 28.7)},
    'GR': {'lat': (34.5, 42.0), 'lng': (19.3, 29.7)},
    'TR': {'lat': (35.8, 42.2), 'lng': (25.6, 44.8)},
    'SE': {'lat': (55.2, 69.1), 'lng': (11.0, 24.2)},
    'NO': {'lat': (57.9, 71.2), 'lng': (4.5, 31.2)},
    'DK': {'lat': (54.5, 57.8), 'lng': (8.0, 15.2)},
    'FI': {'lat': (59.7, 70.1), 'lng': (20.5, 31.6)},
    'IE': {'lat': (51.3, 55.5), 'lng': (-10.7, -5.3)},
    'PT': {'lat': (32.0, 42.2), 'lng': (-31.5, -6.1)},
    'UA': {'lat': (44.3, 52.4), 'lng': (22.1, 40.2)},
    'ZA': {'lat': (-35.0, -22.0), 'lng': (16.4, 33.0)},
    'AU': {'lat': (-44.0, -10.0), 'lng': (112.0, 154.0)},
    'NZ': {'lat': (-48.0, -34.0), 'lng': (166.0, 179.0)},
    'BR': {'lat': (-34.0, 6.0), 'lng': (-74.0, -34.0)},
    'AR': {'lat': (-56.0, -21.0), 'lng': (-74.0, -53.0)},
    'JP': {'lat': (24.0, 46.0), 'lng': (122.0, 154.0)},
    'IS': {'lat': (63.0, 67.0), 'lng': (-25.0, -13.0)},
    'RS': {'lat': (42.2, 46.2), 'lng': (18.8, 23.0)},
    'BA': {'lat': (42.5, 45.3), 'lng': (15.7, 19.7)},
    'SI': {'lat': (45.4, 46.9), 'lng': (13.3, 16.6)},
    'MK': {'lat': (40.8, 42.4), 'lng': (20.4, 23.1)},
    'AL': {'lat': (39.6, 42.7), 'lng': (19.2, 21.1)},
    'XK': {'lat': (41.8, 43.3), 'lng': (20.0, 21.8)},
    'MD': {'lat': (45.4, 48.5), 'lng': (26.6, 30.2)},
    'PA': {'lat': (7.0, 10.0), 'lng': (-83.0, -77.0)},
    'PE': {'lat': (-18.5, -0.0), 'lng': (-81.5, -68.5)},
    'VE': {'lat': (0.5, 12.5), 'lng': (-73.5, -59.5)},
    'DO': {'lat': (17.4, 20.0), 'lng': (-72.0, -68.2)},
    'NI': {'lat': (10.7, 15.0), 'lng': (-87.7, -82.5)},
    'SR': {'lat': (1.8, 6.1), 'lng': (-58.1, -53.9)},
    'CO': {'lat': (-4.3, 13.5), 'lng': (-79.0, -66.8)},
    'CR': {'lat': (8.0, 11.2), 'lng': (-86.0, -82.5)},
    'GT': {'lat': (13.7, 17.8), 'lng': (-92.3, -88.2)},
    'SV': {'lat': (13.1, 14.5), 'lng': (-90.2, -87.7)},
    'BB': {'lat': (13.0, 13.4), 'lng': (-59.7, -59.4)},
    'UY': {'lat': (-35.0, -30.0), 'lng': (-58.5, -53.0)},
    'BO': {'lat': (-23.0, -9.5), 'lng': (-69.7, -57.4)},
    'EC': {'lat': (-5.1, 1.7), 'lng': (-81.1, -75.2)},
    'HN': {'lat': (12.9, 16.5), 'lng': (-89.4, -83.1)},
    'CU': {'lat': (19.8, 23.3), 'lng': (-85.0, -74.1)},
    'CL': {'lat': (-56.0, -17.5), 'lng': (-75.7, -66.4)},
    'MX': {'lat': (14.0, 33.0), 'lng': (-118.0, -86.0)},
    'UZ': {'lat': (37.2, 45.6), 'lng': (56.0, 73.1)},
    'AM': {'lat': (38.8, 41.3), 'lng': (43.4, 46.6)},
    'EG': {'lat': (22.0, 31.7), 'lng': (24.7, 36.9)},
    'MA': {'lat': (27.6, 35.9), 'lng': (-13.2, -1.0)},
    'IL': {'lat': (29.5, 33.4), 'lng': (34.2, 35.9)},
    'LB': {'lat': (33.0, 34.7), 'lng': (35.1, 36.7)},
    'SD': {'lat': (8.6, 22.2), 'lng': (21.8, 38.6)},
    'SN': {'lat': (12.3, 16.7), 'lng': (-17.6, -11.3)},
    'CM': {'lat': (1.6, 13.1), 'lng': (8.5, 16.2)},
    'TD': {'lat': (7.4, 23.5), 'lng': (13.4, 24.0)},
    'AO': {'lat': (-18.0, -4.4), 'lng': (11.6, 24.1)},
    'YE': {'lat': (12.0, 19.0), 'lng': (42.5, 54.5)},
    'KH': {'lat': (10.4, 14.7), 'lng': (102.3, 107.6)},
    'TW': {'lat': (21.9, 25.3), 'lng': (120.0, 122.0)},
    'PH': {'lat': (4.5, 21.2), 'lng': (116.9, 127.0)},
    'OM': {'lat': (16.6, 26.4), 'lng': (52.0, 59.8)},
    'KE': {'lat': (-4.7, 5.0), 'lng': (33.9, 42.0)},
    'BW': {'lat': (-27.0, -17.8), 'lng': (19.9, 29.4)},
    'MZ': {'lat': (-27.0, -10.5), 'lng': (30.2, 40.8)},
    'ZW': {'lat': (-22.5, -15.6), 'lng': (25.2, 33.1)},
    'UG': {'lat': (-1.5, 4.2), 'lng': (29.6, 35.0)},
    'TZ': {'lat': (-11.8, -1.0), 'lng': (29.3, 40.5)},
    'ZM': {'lat': (-18.1, -8.2), 'lng': (21.9, 33.5)},
    'NA': {'lat': (-29.0, -16.9), 'lng': (11.7, 25.3)},
    'NG': {'lat': (4.3, 13.9), 'lng': (2.7, 14.7)},
    'MY': {'lat': (0.8, 7.4), 'lng': (99.6, 119.3)},
    'TH': {'lat': (5.6, 20.5), 'lng': (97.3, 105.6)},
    'SG': {'lat': (1.2, 1.5), 'lng': (103.6, 104.0)},
    'SA': {'lat': (16.3, 32.2), 'lng': (34.5, 55.7)},
    'QA': {'lat': (24.5, 26.2), 'lng': (50.7, 51.7)},
    'IQ': {'lat': (29.0, 37.4), 'lng': (38.8, 48.6)},
    'JO': {'lat': (29.2, 33.4), 'lng': (34.9, 39.3)},
    'BH': {'lat': (25.8, 26.3), 'lng': (50.3, 50.7)},
    'AE': {'lat': (22.6, 26.1), 'lng': (51.5, 56.4)},
    'LI': {'lat': (47.0, 47.3), 'lng': (9.5, 9.6)},
    'GG': {'lat': (49.4, 49.5), 'lng': (-2.7, -2.5)},
    'JE': {'lat': (49.1, 49.3), 'lng': (-2.3, -2.0)},
    'GE': {'lat': (41.0, 43.6), 'lng': (40.0, 46.7)},
    'AZ': {'lat': (38.4, 41.9), 'lng': (44.7, 50.7)},
    'ET': {'lat': (3.4, 14.9), 'lng': (33.0, 48.0)},
    'KG': {'lat': (39.2, 43.3), 'lng': (69.2, 80.3)},
    'GN': {'lat': (7.2, 12.7), 'lng': (-15.1, -7.6)},
    'MR': {'lat': (14.7, 27.3), 'lng': (-17.1, -4.8)},
    'MM': {'lat': (9.8, 28.5), 'lng': (92.2, 101.2)},
    'CI': {'lat': (4.3, 10.7), 'lng': (-8.6, -2.5)},
    'RE': {'lat': (-21.4, -20.8), 'lng': (55.2, 55.9)},
    'NC': {'lat': (-23.0, -19.5), 'lng': (163.5, 168.5)},
    'LT': {'lat': (53.9, 56.5), 'lng': (21.0, 26.8)},
}


def infer_country_from_coords(lat, lng):
    """Try to infer country code from coordinates."""
    for cc, bounds in COUNTRY_BOUNDS.items():
        if bounds['lat'][0] <= lat <= bounds['lat'][1] and bounds['lng'][0] <= lng <= bounds['lng'][1]:
            return cc
    return None


def infer_country_from_city(city):
    """Try to infer country from city text (for INTL/Argo dealers)."""
    city_lower = city.lower().strip()

    # Direct country name matches
    mappings = {
        'uzbekistan': 'UZ', 'kosovo': 'XK', 'slovenija': 'SI', 'slovenia': 'SI',
        'holland': 'NL', 'İstanbul': 'TR', 'istanbul': 'TR', 'moldova': 'MD',
        'yemen': 'YE', 'angola': 'AO', 'israel': 'IL', 'senegal': 'SN',
        'cameroon': 'CM', 'chad': 'TD', 'sudan': 'SD', 'armenia': 'AM',
        'egypt': 'EG', 'morocco': 'MA', 'taiwan': 'TW', 'lebanon': 'LB',
        'namibia': 'NA', 'perú': 'PE', 'peru': 'PE',
        'republic of serbia': 'RS', 'serbia': 'RS',
        'bosnia and herzegovina': 'BA', 'bosnia': 'BA',
        'the republic of north macedonia': 'MK', 'north macedonia': 'MK',
        'panamá': 'PA', 'panama': 'PA',
        'república del perú': 'PE',
        'rep. dominicana': 'DO', 'dominican republic': 'DO',
        'república de nicaragua': 'NI', 'nicaragua': 'NI',
        'republiek suriname': 'SR', 'suriname': 'SR',
        'república de colombia': 'CO', 'colombia': 'CO',
        'república de costa rica': 'CR', 'costa rica': 'CR',
        'barbados': 'BB', 'canelones': 'UY', 'colonia nicolich': 'UY',
        'georgia': 'GE', 'azerbaijan': 'AZ', 'cyprus': 'CY',
        'ethiopia': 'ET', 'kazakistan': 'KZ', 'kazakhstan': 'KZ',
        'kyrgyz republic': 'KG', 'kyrgyzstan': 'KG',
        'guinea': 'GN', 'mauritania': 'MR', 'myanmar': 'MM',
        "cote d'ivoire": 'CI', 'ivory coast': 'CI',
        'biržai': 'LT',  # Lithuanian city
        'isole reunion': 'RE', 'réunion': 'RE', 'reunion': 'RE',
        'nouvelle-caledonie': 'NC', 'nouvelle calédonie': 'NC',
    }

    for pattern, cc in mappings.items():
        if pattern in city_lower:
            return cc

    return None


def fix_argo_ar_country(row):
    """Fix Argo dealers coded as AR that are really in other Latin American countries."""
    # Check city text FIRST for cases where coords overlap (e.g. UY/AR border)
    city_inferred = infer_country_from_city(row['city'])
    if city_inferred and city_inferred != 'AR':
        row['country_code'] = city_inferred
        row['country_name'] = CC_TO_NAME.get(city_inferred, city_inferred)
        return row

    try:
        lat = float(row['latitude'])
        lng = float(row['longitude'])
    except (ValueError, TypeError):
        return row

    # If coords are actually in Argentina, keep it
    if -56 <= lat <= -21 and -74 <= lng <= -53:
        return row

    # Try to infer from coords
    inferred = infer_country_from_coords(lat, lng)
    if inferred:
        row['country_code'] = inferred
        row['country_name'] = CC_TO_NAME.get(inferred, inferred)
        return row

    # Try from city text
    inferred = infer_country_from_city(row['city'])
    if inferred:
        row['country_code'] = inferred
        row['country_name'] = CC_TO_NAME.get(inferred, inferred)
        return row

    return row


def fix_argo_de_to_at(row):
    """Fix Argo dealers coded as DE that are actually in Austria."""
    try:
        lat = float(row['latitude'])
        lng = float(row['longitude'])
    except (ValueError, TypeError):
        return row

    # If within Austria bounds but not Germany
    at_bounds = COUNTRY_BOUNDS['AT']
    de_bounds = COUNTRY_BOUNDS['DE']

    in_at = at_bounds['lat'][0] <= lat <= at_bounds['lat'][1] and at_bounds['lng'][0] <= lng <= at_bounds['lng'][1]
    in_de = de_bounds['lat'][0] <= lat <= de_bounds['lat'][1] and de_bounds['lng'][0] <= lng <= de_bounds['lng'][1]

    if in_at and not in_de:
        row['country_code'] = 'AT'
        row['country_name'] = 'Austria'
    elif in_at and in_de:
        # Ambiguous (overlap region) - check city names
        city_lower = row['city'].lower()
        austrian_hints = ['niederösterreich', 'oberösterreich', 'steiermark', 'kärnten',
                          'wolfsberg', 'burgenland', 'podersdorf', 'lebring']
        for hint in austrian_hints:
            if hint in city_lower:
                row['country_code'] = 'AT'
                row['country_name'] = 'Austria'
                break

    return row


def fix_sdf_regional_codes(row):
    """Fix SDF SAME dealers with regional codes (EU, EA, FE, LA)."""
    cc = row['country_code']
    if cc not in ('EU', 'EA', 'FE', 'LA'):
        return row

    # Try coords first
    try:
        lat = float(row['latitude'])
        lng = float(row['longitude'])
        inferred = infer_country_from_coords(lat, lng)
        if inferred:
            row['country_code'] = inferred
            row['country_name'] = CC_TO_NAME.get(inferred, inferred)
            return row
    except (ValueError, TypeError):
        pass

    # Try city text
    city = row['city']
    city_lower = city.lower()
    mappings = {
        'serbia': 'RS', 'skopje': 'MK', 'budapest': 'HU', 'gyöngyös': 'HU',
        'nyíregyháza': 'HU', 'gradiška': 'BA', 'korce': 'AL', 'athens': 'GR',
        'thessaloniki': 'GR', 'melbourne': 'AU', 'morrinsville': 'NZ',
        'quezon city': 'PH', 'kaohsiung': 'TW', 'medellín': 'CO', 'durán': 'EC',
        'managua': 'NI', 'havana': 'CU', 'san luis': 'AR', 'san fernando': 'CL',
        'san martin': 'PE', 'la victoria': 'PE', 'ciudad de panama': 'PA',
        'otopeni': 'RO',
    }
    for pattern, cc_new in mappings.items():
        if pattern in city_lower:
            row['country_code'] = cc_new
            row['country_name'] = CC_TO_NAME.get(cc_new, cc_new)
            return row

    if 'slovenia' in city_lower or 'slovenija' in city_lower:
        row['country_code'] = 'SI'
        row['country_name'] = 'Slovenia'

    return row


def fix_intl_country(row):
    """Fix INTL-coded Argo dealers by inferring from coords/city."""
    if row['country_code'] != 'INTL':
        return row

    # Try coords
    try:
        lat = float(row['latitude'])
        lng = float(row['longitude'])
        inferred = infer_country_from_coords(lat, lng)
        if inferred:
            row['country_code'] = inferred
            row['country_name'] = CC_TO_NAME.get(inferred, inferred)
            return row
    except (ValueError, TypeError):
        pass

    # Try city
    inferred = infer_country_from_city(row['city'])
    if inferred:
        row['country_code'] = inferred
        row['country_name'] = CC_TO_NAME.get(inferred, inferred)

    return row


def fix_gb_wrong_coords(row):
    """Fix GB dealer 'Farm and Fleet Services' with Australian coordinates."""
    if row['country_code'] == 'GB' and row['dealer_name'] == 'Farm and Fleet Services':
        try:
            lat = float(row['latitude'])
            if lat < 0:
                # These coords are in Tasmania, Australia - clearly wrong
                # Clear them rather than guess
                row['latitude'] = ''
                row['longitude'] = ''
        except (ValueError, TypeError):
            pass
    return row


def fix_zero_coords(row):
    """Clear obviously wrong (0,0) coordinates."""
    try:
        lat = float(row['latitude'])
        lng = float(row['longitude'])
        if abs(lat) < 0.5 and abs(lng) < 0.5:
            row['latitude'] = ''
            row['longitude'] = ''
    except (ValueError, TypeError):
        pass
    return row


def remove_blank_rows(row):
    """Return False for rows that should be removed (blank name + blank coords)."""
    if not row['dealer_name'].strip() and not row['latitude'].strip():
        return False
    return True


def fix_country_name(row):
    """Ensure country_name is populated where country_code is known."""
    cc = row['country_code']
    if cc and cc in CC_TO_NAME and not row['country_name'].strip():
        row['country_name'] = CC_TO_NAME[cc]
    return row


def remove_exact_duplicates(rows):
    """Remove exact duplicates (same brand, name, city, address)."""
    seen = set()
    out = []
    removed = 0
    for r in rows:
        key = (r['brand'], r['dealer_name'].strip().lower(), r['city'].strip().lower(), r['address'].strip().lower())
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        out.append(r)
    return out, removed


def main():
    print("Loading data...")
    with open(INPUT_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Loaded {len(rows)} rows")

    changes = Counter()

    # Apply fixes
    cleaned = []
    for r in rows:
        original_cc = r['country_code']
        original_lat = r['latitude']

        # Fix Argo AR miscodings
        if r['parent_company'] == 'Argo Tractors' and r['country_code'] == 'AR':
            r = fix_argo_ar_country(r)
            if r['country_code'] != 'AR':
                changes['argo_ar_fixed'] += 1

        # Fix Argo DE -> AT
        if r['parent_company'] == 'Argo Tractors' and r['country_code'] == 'DE':
            r = fix_argo_de_to_at(r)
            if r['country_code'] == 'AT':
                changes['argo_de_to_at'] += 1

        # Fix SDF regional codes
        if r['country_code'] in ('EU', 'EA', 'FE', 'LA'):
            old_cc = r['country_code']
            r = fix_sdf_regional_codes(r)
            if r['country_code'] != old_cc:
                changes['sdf_regional_fixed'] += 1

        # Fix INTL codes
        if r['country_code'] == 'INTL':
            r = fix_intl_country(r)
            if r['country_code'] != 'INTL':
                changes['intl_fixed'] += 1

        # Fix GB wrong coords
        r = fix_gb_wrong_coords(r)
        if original_lat and not r['latitude']:
            changes['gb_coords_cleared'] += 1

        # Fix zero coords
        old_lat = r['latitude']
        r = fix_zero_coords(r)
        if old_lat and not r['latitude']:
            changes['zero_coords_cleared'] += 1

        # Ensure country name
        r = fix_country_name(r)

        # Remove blank rows
        if remove_blank_rows(r):
            cleaned.append(r)
        else:
            changes['blank_rows_removed'] += 1

    # Remove exact duplicates
    cleaned, dup_count = remove_exact_duplicates(cleaned)
    changes['exact_duplicates_removed'] = dup_count

    # Write output
    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(cleaned)

    print(f"\nWrote {len(cleaned)} cleaned dealers to {OUTPUT_PATH}")
    print(f"\nChanges applied:")
    for change, count in changes.most_common():
        print(f"  {change}: {count}")

    # Summary stats
    brand_counts = Counter(r['brand'] for r in cleaned)
    country_counts = Counter(r['country_code'] for r in cleaned)
    has_coords = sum(1 for r in cleaned if r['latitude'] and r['longitude'])

    print(f"\nCleaned dataset summary:")
    print(f"  Total: {len(cleaned)}")
    print(f"  With coords: {has_coords}")
    print(f"  Brands: {len(brand_counts)}")
    print(f"  Countries: {len(country_counts)}")

    # Check remaining non-ISO codes
    remaining_non = [cc for cc in country_counts if cc not in CC_TO_NAME and cc != '']
    if remaining_non:
        print(f"\n  Remaining non-standard codes: {remaining_non}")
        for cc in remaining_non:
            print(f"    {cc}: {country_counts[cc]}")


if __name__ == '__main__':
    main()
