#!/usr/bin/env python3
"""Merge all tractor dealer CSVs into a unified dataset."""

import csv
import json
import os
import hashlib
from collections import Counter

RAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'raw')
OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'processed')
os.makedirs(OUT_DIR, exist_ok=True)

# Unified output columns
COLUMNS = [
    'brand', 'parent_company', 'dealer_name', 'address', 'city',
    'state_region', 'country_code', 'country_name', 'postal_code',
    'latitude', 'longitude', 'phone', 'email', 'website',
    'dealer_type', 'services_offered',
]

PARENT_COMPANY = {
    'John Deere': 'Deere & Company',
    'CLAAS': 'CLAAS',
    'McCormick': 'Argo Tractors',
    'Landini': 'Argo Tractors',
    'Fendt': 'AGCO',
    'Massey Ferguson': 'AGCO',
    'Valtra': 'AGCO',
    'Deutz-Fahr': 'SDF Group',
    'SAME': 'SDF Group',
    'Lamborghini Tractors': 'SDF Group',
    'Case IH': 'CNH Industrial',
    'New Holland': 'CNH Industrial',
    'STEYR': 'CNH Industrial',
    'Kubota': 'Kubota',
}

# ISO country code -> name mapping (common ones)
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
}

# Reverse: name -> code
NAME_TO_CC = {v.lower(): k for k, v in CC_TO_NAME.items()}
NAME_TO_CC.update({
    'united states': 'US', 'usa': 'US', 'u.s.a.': 'US',
    'uk': 'GB', 'england': 'GB', 'great britain': 'GB',
    'deutschland': 'DE', 'bundesrepublik deutschland': 'DE',
    'frankreich': 'FR', 'italien': 'IT', 'spanien': 'ES',
    'schweiz': 'CH', 'suisse': 'CH', 'svizzera': 'CH',
    'osterreich': 'AT', 'oesterreich': 'AT',
    'tschechische republik': 'CZ', 'czechia': 'CZ',
    'turkiye': 'TR', 'turkei': 'TR',
    'republic of ireland': 'IE', 'eire': 'IE',
    'south korea': 'KR', 'republic of korea': 'KR',
    'russian federation': 'RU',
    'the netherlands': 'NL', 'holland': 'NL',
    'republic of south africa': 'ZA',
})


def normalize_country(raw_code, raw_name=''):
    """Return (code, name) tuple."""
    code = (raw_code or '').strip().upper()
    name = (raw_name or '').strip()

    # If we have a valid 2-letter code
    if len(code) == 2 and code in CC_TO_NAME:
        return code, CC_TO_NAME[code]

    # Try to derive code from name
    if name:
        lname = name.lower().strip()
        if lname in NAME_TO_CC:
            code = NAME_TO_CC[lname]
            return code, CC_TO_NAME.get(code, name)

    # If code looks like a 2-letter code but not in our map
    if len(code) == 2:
        return code, name or code

    # Try code as a name
    if code:
        lcode = code.lower()
        if lcode in NAME_TO_CC:
            c = NAME_TO_CC[lcode]
            return c, CC_TO_NAME.get(c, code)

    return code, name


def fingerprint(brand, name, lat, lng):
    """Create dedup fingerprint."""
    name_clean = (name or '').strip().lower()
    try:
        lat_r = f'{float(lat):.3f}'
    except (ValueError, TypeError):
        lat_r = ''
    try:
        lng_r = f'{float(lng):.3f}'
    except (ValueError, TypeError):
        lng_r = ''
    raw = f'{brand}|{name_clean}|{lat_r}|{lng_r}'
    return hashlib.md5(raw.encode()).hexdigest()


def read_csv(filename):
    path = os.path.join(RAW_DIR, filename)
    if not os.path.exists(path):
        print(f'  SKIP {filename} (not found)')
        return []
    with open(path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


def process_deere(rows):
    out = []
    for r in rows:
        cc, cn = normalize_country(r.get('country', ''))
        out.append({
            'brand': 'John Deere',
            'parent_company': 'Deere & Company',
            'dealer_name': r.get('dealer_name', ''),
            'address': r.get('address', ''),
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def process_claas(rows):
    out = []
    for r in rows:
        cc, cn = normalize_country(r.get('country', ''), r.get('country_name', ''))
        addr = r.get('address', '')
        house = r.get('house_number', '')
        if house and house not in addr:
            addr = f'{addr} {house}'.strip()
        out.append({
            'brand': 'CLAAS',
            'parent_company': 'CLAAS',
            'dealer_name': r.get('dealer_name', ''),
            'address': addr,
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def process_argo(rows):
    out = []
    for r in rows:
        brand = r.get('brand', '').strip()
        if brand.lower() == 'mccormick':
            brand = 'McCormick'
        elif brand.lower() == 'landini':
            brand = 'Landini'
        cc, cn = normalize_country(r.get('country', ''))
        out.append({
            'brand': brand,
            'parent_company': 'Argo Tractors',
            'dealer_name': r.get('dealer_name', ''),
            'address': r.get('address', ''),
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def normalize_sdf_brand(raw):
    """Extract primary SDF brand from compound names like 'Deutz-Fahr; Lamborghini Trattori; SAME'."""
    raw = raw.strip()
    # Map compound to primary brand
    if 'Deutz-Fahr' in raw or 'Deutz' in raw:
        return 'Deutz-Fahr'
    if 'SAME' in raw:
        return 'SAME'
    if 'Lamborghini' in raw:
        return 'Lamborghini Tractors'
    return raw


def process_sdf(rows):
    out = []
    for r in rows:
        brand = normalize_sdf_brand(r.get('brand', ''))
        cc, cn = normalize_country(r.get('country', ''))
        out.append({
            'brand': brand,
            'parent_company': 'SDF Group',
            'dealer_name': r.get('dealer_name', ''),
            'address': r.get('address', ''),
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def process_agco(rows):
    """Process AGCO combined or individual brand files."""
    out = []
    for r in rows:
        brand = r.get('brand', '').strip()
        # Normalize brand names
        if brand.lower() in ('fendt', 'ft', 'fe'):
            brand = 'Fendt'
        elif 'massey' in brand.lower() or brand.upper() == 'MF':
            brand = 'Massey Ferguson'
        elif 'valtra' in brand.lower() or brand.upper() == 'VL':
            brand = 'Valtra'

        cc_field = r.get('country_code', '') or r.get('country', '')
        name_field = r.get('country', '') if r.get('country_code') else ''
        cc, cn = normalize_country(cc_field, name_field)

        out.append({
            'brand': brand,
            'parent_company': PARENT_COMPANY.get(brand, 'AGCO'),
            'dealer_name': r.get('dealer_name', ''),
            'address': r.get('address', ''),
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def process_cnh_json():
    """Read CNH progress JSON if it exists."""
    path = os.path.join(RAW_DIR, 'cnh_scrape_progress.json')
    if not os.path.exists(path):
        print('  SKIP cnh_scrape_progress.json (not found)')
        return []

    with open(path) as f:
        data = json.load(f)

    dealers = data.get('dealers', {})
    if isinstance(dealers, list):
        items = dealers
    else:
        items = list(dealers.values())

    out = []
    for r in items:
        brand = r.get('brand', '').strip()
        if 'new holland' in brand.lower():
            brand = 'New Holland'
        elif 'case' in brand.lower():
            brand = 'Case IH'
        elif 'steyr' in brand.lower():
            brand = 'STEYR'

        cc = r.get('country_code', '')
        cn = r.get('country', '')
        if not cc and cn:
            cc, cn = normalize_country('', cn)
        elif cc:
            cc, cn = normalize_country(cc, cn)

        out.append({
            'brand': brand,
            'parent_company': 'CNH Industrial',
            'dealer_name': r.get('dealer_name', ''),
            'address': r.get('address', ''),
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def process_cnh_csv():
    """Read CNH CSV if it exists."""
    path = os.path.join(RAW_DIR, 'cnh_dealers.csv')
    if not os.path.exists(path):
        return []
    rows = read_csv('cnh_dealers.csv')
    out = []
    for r in rows:
        brand = r.get('brand', '').strip()
        if 'new holland' in brand.lower():
            brand = 'New Holland'
        elif 'case' in brand.lower():
            brand = 'Case IH'
        elif 'steyr' in brand.lower():
            brand = 'STEYR'
        cc, cn = normalize_country(r.get('country_code', '') or r.get('country', ''), r.get('country', ''))
        out.append({
            'brand': brand,
            'parent_company': 'CNH Industrial',
            'dealer_name': r.get('dealer_name', ''),
            'address': r.get('address', ''),
            'city': r.get('city', ''),
            'state_region': r.get('state_region', ''),
            'country_code': cc,
            'country_name': cn,
            'postal_code': r.get('postal_code', ''),
            'latitude': r.get('latitude', ''),
            'longitude': r.get('longitude', ''),
            'phone': r.get('phone', ''),
            'email': r.get('email', ''),
            'website': r.get('website', ''),
            'dealer_type': r.get('dealer_type', ''),
            'services_offered': r.get('services_offered', ''),
        })
    return out


def main():
    all_dealers = []
    seen = set()

    sources = [
        ('deere_dealers.csv', 'John Deere', process_deere),
        ('claas_dealers.csv', 'CLAAS', process_claas),
        ('argo_dealers.csv', 'Argo', process_argo),
        ('sdf_dealers.csv', 'SDF', process_sdf),
        ('agco_dealers_combined.csv', 'AGCO Combined', process_agco),
    ]

    for filename, label, processor in sources:
        print(f'Processing {label} ({filename})...')
        rows = read_csv(filename)
        if rows:
            processed = processor(rows)
            before = len(all_dealers)
            for d in processed:
                fp = fingerprint(d['brand'], d['dealer_name'], d['latitude'], d['longitude'])
                if fp not in seen:
                    seen.add(fp)
                    all_dealers.append(d)
            added = len(all_dealers) - before
            print(f'  {len(rows)} rows -> {len(processed)} processed -> {added} new (deduped)')

    # CNH: try CSV first, fall back to JSON
    print('Processing CNH (CSV or JSON)...')
    cnh = process_cnh_csv()
    if not cnh:
        cnh = process_cnh_json()
        print(f'  Using JSON progress file')
    before = len(all_dealers)
    for d in cnh:
        fp = fingerprint(d['brand'], d['dealer_name'], d['latitude'], d['longitude'])
        if fp not in seen:
            seen.add(fp)
            all_dealers.append(d)
    print(f'  {len(cnh)} CNH dealers -> {len(all_dealers) - before} new')

    # Kubota (if exists)
    kubota_path = os.path.join(RAW_DIR, 'kubota_dealers.csv')
    if os.path.exists(kubota_path):
        print('Processing Kubota...')
        rows = read_csv('kubota_dealers.csv')
        before = len(all_dealers)
        for r in rows:
            cc, cn = normalize_country(r.get('country_code', '') or r.get('country', ''))
            d = {
                'brand': 'Kubota',
                'parent_company': 'Kubota',
                'dealer_name': r.get('dealer_name', ''),
                'address': r.get('address', ''),
                'city': r.get('city', ''),
                'state_region': r.get('state_region', ''),
                'country_code': cc,
                'country_name': cn,
                'postal_code': r.get('postal_code', ''),
                'latitude': r.get('latitude', ''),
                'longitude': r.get('longitude', ''),
                'phone': r.get('phone', ''),
                'email': r.get('email', ''),
                'website': r.get('website', ''),
                'dealer_type': r.get('dealer_type', ''),
                'services_offered': r.get('services_offered', ''),
            }
            fp = fingerprint(d['brand'], d['dealer_name'], d['latitude'], d['longitude'])
            if fp not in seen:
                seen.add(fp)
                all_dealers.append(d)
        print(f'  {len(rows)} rows -> {len(all_dealers) - before} new')
    else:
        print('  SKIP kubota_dealers.csv (not found)')

    # Write unified CSV
    out_path = os.path.join(OUT_DIR, 'all_dealers.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(all_dealers)
    print(f'\nWrote {len(all_dealers)} dealers to {out_path}')

    # Stats
    brand_counts = Counter(d['brand'] for d in all_dealers)
    parent_counts = Counter(d['parent_company'] for d in all_dealers)
    country_counts = Counter(d['country_code'] for d in all_dealers)
    has_coords = sum(1 for d in all_dealers if d['latitude'] and d['longitude'])

    print(f'\n{"="*60}')
    print(f'SUMMARY: {len(all_dealers)} total dealers')
    print(f'With coordinates: {has_coords} ({100*has_coords/len(all_dealers):.1f}%)')

    print(f'\nBy Parent Company:')
    for co, n in parent_counts.most_common():
        print(f'  {co}: {n}')

    print(f'\nBy Brand:')
    for b, n in brand_counts.most_common():
        print(f'  {b}: {n}')

    print(f'\nTop 25 Countries:')
    for cc, n in country_counts.most_common(25):
        name = CC_TO_NAME.get(cc, cc)
        print(f'  {cc} ({name}): {n}')

    # Write JSON stats
    stats = {
        'total_dealers': len(all_dealers),
        'with_coordinates': has_coords,
        'by_parent_company': dict(parent_counts.most_common()),
        'by_brand': dict(brand_counts.most_common()),
        'by_country': {cc: {'count': n, 'name': CC_TO_NAME.get(cc, cc)}
                       for cc, n in country_counts.most_common()},
    }
    stats_path = os.path.join(OUT_DIR, 'summary_stats.json')
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f'\nWrote stats to {stats_path}')


if __name__ == '__main__':
    main()
