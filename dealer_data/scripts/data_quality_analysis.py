#!/usr/bin/env python3
"""Comprehensive data quality analysis of the unified tractor dealer dataset."""

import csv
import json
import os
import sys
from collections import Counter, defaultdict

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
RAW_DIR = os.path.join(BASE_DIR, 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, 'processed')

CSV_PATH = os.path.join(PROCESSED_DIR, 'all_dealers.csv')

# ======== Expected coordinate bounding boxes per country ========
COUNTRY_BOUNDS = {
    'US': {'lat': (24.0, 72.0), 'lng': (-180.0, -65.0)},  # incl Alaska/Hawaii
    'CA': {'lat': (41.0, 84.0), 'lng': (-141.0, -52.0)},
    'MX': {'lat': (14.0, 33.0), 'lng': (-118.0, -86.0)},
    'DE': {'lat': (47.0, 55.5), 'lng': (5.5, 15.5)},
    'FR': {'lat': (41.0, 51.5), 'lng': (-5.5, 10.0)},
    'IT': {'lat': (36.0, 47.5), 'lng': (6.5, 18.6)},
    'ES': {'lat': (27.5, 44.0), 'lng': (-18.5, 4.5)},  # incl Canaries
    'GB': {'lat': (49.5, 61.0), 'lng': (-8.5, 2.0)},
    'AT': {'lat': (46.3, 49.1), 'lng': (9.5, 17.2)},
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
    'PT': {'lat': (32.0, 42.2), 'lng': (-31.5, -6.1)},  # incl Azores
    'UA': {'lat': (44.3, 52.4), 'lng': (22.1, 40.2)},
    'ZA': {'lat': (-35.0, -22.0), 'lng': (16.4, 33.0)},
    'AU': {'lat': (-44.0, -10.0), 'lng': (112.0, 154.0)},
    'NZ': {'lat': (-48.0, -34.0), 'lng': (166.0, 179.0)},
    'BR': {'lat': (-34.0, 6.0), 'lng': (-74.0, -34.0)},
    'AR': {'lat': (-56.0, -21.0), 'lng': (-74.0, -53.0)},
    'JP': {'lat': (24.0, 46.0), 'lng': (122.0, 154.0)},
    'IS': {'lat': (63.0, 67.0), 'lng': (-25.0, -13.0)},
}

# US continental only (excluding Alaska/Hawaii) for tighter checks
US_CONTINENTAL = {'lat': (24.0, 50.0), 'lng': (-125.0, -65.0)}

def load_data():
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


def analyze_coverage(rows):
    """Analyze brand and country coverage gaps."""
    results = {}

    # Brand counts
    brand_counts = Counter(r['brand'] for r in rows)
    parent_counts = Counter(r['parent_company'] for r in rows)
    country_counts = Counter(r['country_code'] for r in rows)

    # Brand x Country matrix for major markets
    major_markets = ['US', 'DE', 'FR', 'IT', 'GB', 'ES', 'AT', 'PL', 'NL', 'SE', 'NO', 'FI', 'CA', 'AU', 'ZA', 'TR', 'CZ', 'RO', 'HU', 'BG', 'GR']
    brand_country = defaultdict(lambda: defaultdict(int))
    for r in rows:
        brand_country[r['brand']][r['country_code']] += 1

    results['brand_counts'] = dict(brand_counts.most_common())
    results['parent_counts'] = dict(parent_counts.most_common())
    results['country_counts'] = dict(country_counts.most_common())
    results['brand_country_matrix'] = {}
    for brand in sorted(brand_counts.keys()):
        results['brand_country_matrix'][brand] = {cc: brand_country[brand].get(cc, 0) for cc in major_markets}

    # Missing brands
    results['missing_brands'] = ['Kubota']

    # European ag countries with low/no representation
    eu_ag_countries = {
        'RO': 'Romania', 'HU': 'Hungary', 'GR': 'Greece', 'BG': 'Bulgaria',
        'HR': 'Croatia', 'RS': 'Serbia', 'LT': 'Lithuania', 'LV': 'Latvia',
        'EE': 'Estonia', 'SI': 'Slovenia', 'SK': 'Slovakia',
    }
    results['eastern_eu_coverage'] = {}
    for cc, name in eu_ag_countries.items():
        cnt = country_counts.get(cc, 0)
        results['eastern_eu_coverage'][f'{cc} ({name})'] = cnt

    # US brand breakdown
    us_brand = defaultdict(int)
    for r in rows:
        if r['country_code'] == 'US':
            us_brand[r['brand']] += 1
    results['us_brand_counts'] = dict(sorted(us_brand.items(), key=lambda x: -x[1]))

    return results


def analyze_coordinates(rows):
    """Validate coordinates against country bounding boxes."""
    issues = []
    zero_coords = []
    missing_coords = []
    out_of_bounds = []

    for i, r in enumerate(rows):
        lat_s = r.get('latitude', '').strip()
        lng_s = r.get('longitude', '').strip()

        if not lat_s or not lng_s:
            missing_coords.append({
                'row': i + 2,
                'brand': r['brand'],
                'dealer_name': r['dealer_name'],
                'country': r['country_code'],
                'city': r['city'],
            })
            continue

        try:
            lat = float(lat_s)
            lng = float(lng_s)
        except ValueError:
            issues.append({
                'row': i + 2,
                'type': 'invalid_coords',
                'brand': r['brand'],
                'dealer_name': r['dealer_name'],
                'lat': lat_s,
                'lng': lng_s,
            })
            continue

        # Check for (0,0) or near-zero
        if abs(lat) < 0.5 and abs(lng) < 0.5:
            zero_coords.append({
                'row': i + 2,
                'brand': r['brand'],
                'dealer_name': r['dealer_name'],
                'country': r['country_code'],
                'lat': lat,
                'lng': lng,
            })
            continue

        # Check against country bounds
        cc = r['country_code']
        if cc in COUNTRY_BOUNDS:
            bounds = COUNTRY_BOUNDS[cc]
            lat_ok = bounds['lat'][0] <= lat <= bounds['lat'][1]
            lng_ok = bounds['lng'][0] <= lng <= bounds['lng'][1]
            if not lat_ok or not lng_ok:
                out_of_bounds.append({
                    'row': i + 2,
                    'brand': r['brand'],
                    'dealer_name': r['dealer_name'],
                    'country': cc,
                    'city': r['city'],
                    'lat': lat,
                    'lng': lng,
                    'expected_lat': bounds['lat'],
                    'expected_lng': bounds['lng'],
                })

    return {
        'missing_coords': missing_coords,
        'zero_coords': zero_coords,
        'out_of_bounds': out_of_bounds,
        'invalid_format': issues,
    }


def analyze_duplicates(rows):
    """Find potential duplicates."""
    exact_dupes = []
    multi_brand = []

    # Exact duplicates: same name, same city, same address
    seen = {}
    for i, r in enumerate(rows):
        key = (r['dealer_name'].strip().lower(), r['city'].strip().lower(), r['address'].strip().lower(), r['brand'])
        if key in seen:
            exact_dupes.append({
                'row': i + 2,
                'first_row': seen[key] + 2,
                'brand': r['brand'],
                'dealer_name': r['dealer_name'],
                'city': r['city'],
                'address': r['address'],
            })
        else:
            seen[key] = i

    # Multi-brand dealers: same name+city but different brands
    name_city = defaultdict(list)
    for i, r in enumerate(rows):
        key = (r['dealer_name'].strip().lower(), r['city'].strip().lower(), r['country_code'])
        name_city[key].append({
            'row': i + 2,
            'brand': r['brand'],
            'dealer_name': r['dealer_name'],
            'city': r['city'],
            'country': r['country_code'],
        })

    for key, entries in name_city.items():
        brands = set(e['brand'] for e in entries)
        if len(brands) > 1:
            multi_brand.append({
                'dealer_name': entries[0]['dealer_name'],
                'city': entries[0]['city'],
                'country': entries[0]['country'],
                'brands': sorted(brands),
                'count': len(entries),
            })

    # Very close coordinates (within ~100m) with same brand but different names
    # This would catch renamed dealers
    close_coords = []
    # Build spatial index by brand
    brand_coords = defaultdict(list)
    for i, r in enumerate(rows):
        try:
            lat = float(r['latitude'])
            lng = float(r['longitude'])
            brand_coords[r['brand']].append((lat, lng, i, r['dealer_name']))
        except (ValueError, TypeError):
            pass

    for brand, coords in brand_coords.items():
        coords.sort()
        for j in range(len(coords) - 1):
            lat1, lng1, idx1, name1 = coords[j]
            for k in range(j + 1, min(j + 20, len(coords))):
                lat2, lng2, idx2, name2 = coords[k]
                if abs(lat2 - lat1) > 0.01:
                    break
                if abs(lng2 - lng1) < 0.01:
                    if name1.lower() != name2.lower():
                        close_coords.append({
                            'brand': brand,
                            'name1': name1,
                            'name2': name2,
                            'lat1': lat1, 'lng1': lng1,
                            'lat2': lat2, 'lng2': lng2,
                            'distance_approx_m': int(((lat2 - lat1)**2 + (lng2 - lng1)**2)**0.5 * 111000),
                        })

    return {
        'exact_duplicates': exact_dupes,
        'multi_brand_dealers': multi_brand[:50],  # limit output
        'multi_brand_count': len(multi_brand),
        'close_coords_different_names': close_coords[:30],
    }


def analyze_data_quality(rows):
    """Check data quality issues."""
    issues = {
        'unknown_country_codes': [],
        'argo_country_issues': [],
        'phone_format_issues': [],
        'address_contains_city': [],
        'missing_name': [],
        'missing_critical': [],
        'blank_country': [],
        'non_iso_country_codes': [],
    }

    # Known valid ISO 3166-1 alpha-2 codes (common ones)
    valid_cc = set([
        'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AW', 'AX', 'AZ',
        'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS',
        'BT', 'BW', 'BY', 'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN', 'CO',
        'CR', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ', 'EC', 'EE', 'EG',
        'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK', 'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 'GE', 'GF', 'GG',
        'GH', 'GI', 'GL', 'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM', 'HN',
        'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO',
        'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI',
        'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 'MK', 'ML',
        'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NC',
        'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH',
        'PK', 'PL', 'PM', 'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW', 'SA',
        'SB', 'SC', 'SD', 'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS', 'ST',
        'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF', 'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TR',
        'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI', 'VN',
        'VU', 'WF', 'WS', 'XK', 'YE', 'YT', 'ZA', 'ZM', 'ZW',
    ])

    non_iso = {'INTL', 'EU', 'LA', 'EA', 'FE', 'NA'}  # non-standard codes

    phone_formats = Counter()
    country_code_counts = Counter()

    for i, r in enumerate(rows):
        cc = r['country_code'].strip()
        country_code_counts[cc] += 1

        # Missing name
        if not r['dealer_name'].strip():
            issues['missing_name'].append({
                'row': i + 2,
                'brand': r['brand'],
                'country': cc,
                'city': r['city'],
            })

        # Missing critical fields
        if not r['dealer_name'].strip() and not r['latitude'].strip():
            issues['missing_critical'].append({
                'row': i + 2,
                'brand': r['brand'],
            })

        # Blank country
        if not cc:
            issues['blank_country'].append({
                'row': i + 2,
                'brand': r['brand'],
                'dealer_name': r['dealer_name'],
                'city': r['city'],
                'lat': r['latitude'],
                'lng': r['longitude'],
            })

        # Non-ISO codes
        if cc and cc not in valid_cc:
            issues['non_iso_country_codes'].append({
                'code': cc,
                'row': i + 2,
                'brand': r['brand'],
                'dealer_name': r['dealer_name'],
                'city': r['city'],
            })

        # Argo country issues
        if r['parent_company'] == 'Argo Tractors':
            if cc == 'AR':
                # Check if actually in Argentina by coordinates
                try:
                    lat = float(r['latitude'])
                    lng = float(r['longitude'])
                    # Argentina: lat -56 to -21, lng -74 to -53
                    if not (-56 <= lat <= -21 and -74 <= lng <= -53):
                        issues['argo_country_issues'].append({
                            'row': i + 2,
                            'brand': r['brand'],
                            'dealer_name': r['dealer_name'],
                            'city': r['city'],
                            'claimed_country': 'AR',
                            'lat': lat,
                            'lng': lng,
                            'likely_country': guess_country(lat, lng),
                        })
                except (ValueError, TypeError):
                    pass

        # Phone format
        phone = r.get('phone', '').strip()
        if phone:
            if phone.startswith('+'):
                phone_formats['intl_format'] += 1
            elif phone.startswith('00'):
                phone_formats['00_prefix'] += 1
            elif phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '').isdigit():
                phone_formats['digits_only'] += 1
            else:
                phone_formats['other'] += 1

    return issues, dict(phone_formats)


def guess_country(lat, lng):
    """Very rough country guess from coordinates."""
    for cc, bounds in COUNTRY_BOUNDS.items():
        if bounds['lat'][0] <= lat <= bounds['lat'][1] and bounds['lng'][0] <= lng <= bounds['lng'][1]:
            return cc
    # Regional guesses
    if 0 < lat < 15 and -85 < lng < -75:
        return 'Central America'
    if -5 < lat < 13 and -82 < lng < -66:
        return 'Colombia/Venezuela/Central America'
    if 15 < lat < 25 and -90 < lng < -60:
        return 'Caribbean'
    if lat > 0 and -30 < lng < 60:
        return 'Europe/Africa'
    return 'Unknown'


def analyze_sdf_gaps(rows):
    """Check SDF/Deutz-Fahr US coverage gaps."""
    sdf_us = [r for r in rows if r['parent_company'] == 'SDF Group' and r['country_code'] == 'US']
    df_us = [r for r in rows if r['brand'] == 'Deutz-Fahr' and r['country_code'] == 'US']
    same_us = [r for r in rows if r['brand'] == 'SAME' and r['country_code'] == 'US']

    return {
        'sdf_us_total': len(sdf_us),
        'deutz_fahr_us': len(df_us),
        'same_us': len(same_us),
        'deutz_fahr_us_dealers': [{'name': r['dealer_name'], 'city': r['city'], 'state': r['state_region']} for r in df_us],
        'note': 'Deutz-Fahr US dealer locator was blocked by Cloudflare. Estimate ~50-80 US Deutz-Fahr dealers missing.',
    }


def analyze_cnh_completeness():
    """Check CNH scrape completeness from progress file."""
    path = os.path.join(RAW_DIR, 'cnh_scrape_progress.json')
    if not os.path.exists(path):
        return {'status': 'no progress file'}

    with open(path) as f:
        data = json.load(f)

    completed = data.get('completed', [])
    dealers = data.get('dealers', {})

    # Parse completed tasks
    caseih_us_done = [t for t in completed if t.startswith('caseih_us_')]
    nh_us_done = [t for t in completed if t.startswith('nh_us_')]
    caseih_eu_done = [t for t in completed if t.startswith('caseih_eu_')]
    nh_eu_done = [t for t in completed if t.startswith('nh_eu_')]
    steyr_eu_done = [t for t in completed if t.startswith('steyr_eu_')]

    # Expected EU countries
    eu_countries = ['AL', 'AT', 'BA', 'BE', 'BG', 'CH', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GB',
                    'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LT', 'LU', 'LV', 'MD', 'ME', 'MK', 'NL', 'NO',
                    'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'UA']

    steyr_expected = ['AT', 'BA', 'BE', 'BG', 'CH', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GB',
                      'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LT', 'LU', 'LV', 'MD', 'ME', 'MK', 'NL', 'NO',
                      'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'UA']

    steyr_completed_countries = [t.replace('steyr_eu_country_', '') for t in steyr_eu_done]
    steyr_missing = [c for c in steyr_expected if c not in steyr_completed_countries]

    # Brand breakdown
    brand_counts = Counter()
    country_counts = Counter()
    if isinstance(dealers, dict):
        for d in dealers.values():
            brand_counts[d.get('brand', '')] += 1
            country_counts[d.get('country_code', '')] += 1

    return {
        'total_dealers': len(dealers),
        'tasks_completed': len(completed),
        'caseih_us_states_done': len(caseih_us_done),
        'nh_us_states_done': len(nh_us_done),
        'caseih_eu_countries_done': len(caseih_eu_done),
        'nh_eu_countries_done': len(nh_eu_done),
        'steyr_eu_countries_done': len(steyr_eu_done),
        'steyr_missing_countries': steyr_missing,
        'brand_breakdown': dict(brand_counts.most_common()),
        'top_countries': dict(country_counts.most_common(15)),
        'us_complete': len(caseih_us_done) >= 50 and len(nh_us_done) >= 50,
        'eu_complete': len(caseih_eu_done) >= len(eu_countries) and len(nh_eu_done) >= len(eu_countries),
    }


def analyze_agco_completeness(rows):
    """Check for AGCO capped-at-100 issues in European areas."""
    agco_rows = [r for r in rows if r['parent_company'] == 'AGCO']
    # Check per brand per country counts - if exactly 100, may be capped
    brand_country = defaultdict(lambda: defaultdict(int))
    for r in agco_rows:
        brand_country[r['brand']][r['country_code']] += 1

    capped_suspects = []
    for brand, countries in brand_country.items():
        for cc, count in countries.items():
            if count == 100:
                capped_suspects.append({
                    'brand': brand,
                    'country': cc,
                    'count': count,
                    'note': 'Exactly 100 - possible API cap',
                })

    return {
        'agco_brand_country': {brand: dict(sorted(countries.items(), key=lambda x: -x[1])[:10])
                               for brand, countries in brand_country.items()},
        'capped_suspects': capped_suspects,
    }


def main():
    print("Loading data...")
    rows = load_data()
    print(f"Loaded {len(rows)} rows")

    print("\n1. Coverage analysis...")
    coverage = analyze_coverage(rows)

    print("2. Coordinate validation...")
    coords = analyze_coordinates(rows)

    print("3. Duplicate detection...")
    dupes = analyze_duplicates(rows)

    print("4. Data quality issues...")
    quality_issues, phone_formats = analyze_data_quality(rows)

    print("5. SDF/Deutz-Fahr gap analysis...")
    sdf_gaps = analyze_sdf_gaps(rows)

    print("6. CNH completeness...")
    cnh = analyze_cnh_completeness()

    print("7. AGCO completeness...")
    agco = analyze_agco_completeness(rows)

    # Print summary
    print("\n" + "=" * 70)
    print("DATA QUALITY ANALYSIS RESULTS")
    print("=" * 70)

    print(f"\nTotal dealers: {len(rows)}")
    print(f"Missing coords: {len(coords['missing_coords'])}")
    print(f"Zero coords: {len(coords['zero_coords'])}")
    print(f"Out-of-bounds coords: {len(coords['out_of_bounds'])}")
    print(f"Exact duplicates: {len(dupes['exact_duplicates'])}")
    print(f"Multi-brand dealers: {dupes['multi_brand_count']}")
    print(f"Non-ISO country codes: {len(quality_issues['non_iso_country_codes'])}")
    print(f"Blank country: {len(quality_issues['blank_country'])}")
    print(f"Argo country issues: {len(quality_issues['argo_country_issues'])}")

    # Print problematic records
    print("\n--- OUT OF BOUNDS COORDS (first 30) ---")
    for item in coords['out_of_bounds'][:30]:
        print(f"  Row {item['row']}: {item['brand']} - {item['dealer_name'][:40]} | "
              f"Country={item['country']} City={item['city'][:20]} | "
              f"lat={item['lat']:.4f} lng={item['lng']:.4f} | "
              f"Expected lat={item['expected_lat']} lng={item['expected_lng']}")

    print("\n--- ARGO COUNTRY MISMATCHES ---")
    for item in quality_issues['argo_country_issues']:
        print(f"  Row {item['row']}: {item['brand']} - {item['dealer_name'][:40]} | "
              f"Claimed=AR | lat={item['lat']:.4f} lng={item['lng']:.4f} | "
              f"Likely={item['likely_country']}")

    print("\n--- NON-ISO COUNTRY CODES ---")
    code_summary = Counter(item['code'] for item in quality_issues['non_iso_country_codes'])
    for code, count in code_summary.most_common():
        print(f"  {code}: {count} dealers")
        for item in quality_issues['non_iso_country_codes']:
            if item['code'] == code:
                print(f"    e.g. Row {item['row']}: {item['brand']} - {item['dealer_name'][:40]} ({item['city'][:20]})")
                break

    print("\n--- SDF/DEUTZ-FAHR US COVERAGE ---")
    print(f"  Deutz-Fahr US dealers: {sdf_gaps['deutz_fahr_us']}")
    print(f"  SAME US dealers: {sdf_gaps['same_us']}")
    for d in sdf_gaps['deutz_fahr_us_dealers'][:10]:
        print(f"    {d['name'][:40]} - {d['city']}, {d['state']}")

    print("\n--- CNH SCRAPE STATUS ---")
    print(f"  Total CNH dealers: {cnh.get('total_dealers', 'N/A')}")
    print(f"  Case IH US: {cnh.get('caseih_us_states_done', 'N/A')}/51 states")
    print(f"  New Holland US: {cnh.get('nh_us_states_done', 'N/A')}/51 states")
    print(f"  Case IH EU: {cnh.get('caseih_eu_countries_done', 'N/A')}/36 countries")
    print(f"  New Holland EU: {cnh.get('nh_eu_countries_done', 'N/A')}/36 countries")
    print(f"  STEYR EU: {cnh.get('steyr_eu_countries_done', 'N/A')}/36 countries")
    if cnh.get('steyr_missing_countries'):
        print(f"  STEYR MISSING: {', '.join(cnh['steyr_missing_countries'])}")
    print(f"  CNH Brands: {cnh.get('brand_breakdown', {})}")

    print("\n--- AGCO CAPPED-AT-100 SUSPECTS ---")
    for s in agco['capped_suspects']:
        print(f"  {s['brand']} in {s['country']}: {s['count']} (possible cap)")

    # Save full results as JSON for the report
    full_results = {
        'coverage': coverage,
        'coordinates': {
            'missing_count': len(coords['missing_coords']),
            'zero_count': len(coords['zero_coords']),
            'out_of_bounds_count': len(coords['out_of_bounds']),
            'out_of_bounds': coords['out_of_bounds'][:50],
            'zero_coords': coords['zero_coords'],
            'missing_coords': coords['missing_coords'],
        },
        'duplicates': {
            'exact_count': len(dupes['exact_duplicates']),
            'exact_samples': dupes['exact_duplicates'][:20],
            'multi_brand_count': dupes['multi_brand_count'],
            'multi_brand_samples': dupes['multi_brand_dealers'][:30],
            'close_coords_samples': dupes['close_coords_different_names'],
        },
        'quality': {
            'non_iso_codes': dict(Counter(item['code'] for item in quality_issues['non_iso_country_codes']).most_common()),
            'non_iso_samples': quality_issues['non_iso_country_codes'][:30],
            'blank_country': quality_issues['blank_country'],
            'argo_issues': quality_issues['argo_country_issues'],
            'missing_name': quality_issues['missing_name'],
            'phone_formats': phone_formats,
        },
        'sdf_gaps': sdf_gaps,
        'cnh_status': cnh,
        'agco_status': agco,
    }

    results_path = os.path.join(PROCESSED_DIR, 'quality_analysis_results.json')
    with open(results_path, 'w') as f:
        json.dump(full_results, f, indent=2, default=str)
    print(f"\nFull results saved to {results_path}")

    return full_results


if __name__ == '__main__':
    main()
