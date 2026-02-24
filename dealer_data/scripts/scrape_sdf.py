#!/usr/bin/env python3
"""
Scrape SDF Group dealer/service locations.
Brands: Deutz-Fahr, SAME, Lamborghini Tractors.

Two data sources:
1. Lamborghini Tractors Joomla API (com_dealerlocator) - returns all 3 SDF brands
2. SAME Nuxt SSR pages - locale-specific dealer data embedded in page HTML

The Lamborghini API returns the canonical SDF dealer database (~451 dealers).
The SAME Nuxt pages have locale-specific views (~618 unique IDs across all locales).
We scrape both and merge/deduplicate.
"""

import requests
import re
import json
import csv
import time
import html as html_mod
from pathlib import Path
from collections import OrderedDict

# Output paths
SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR.parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = RAW_DIR / "sdf_dealers.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

CSV_FIELDS = [
    "brand", "dealer_name", "address", "city", "state_region", "country",
    "postal_code", "latitude", "longitude", "phone", "fax", "email",
    "website", "dealer_type", "services_offered", "dealer_id", "sap_code",
    "source"
]


def scrape_lamborghini_api():
    """
    Scrape the Lamborghini Tractors Joomla API which serves all SDF brands.
    Uses a large radius search from center of Europe to get all dealers globally.
    """
    print("=" * 60)
    print("Scraping SDF via Lamborghini Tractors API...")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)
    session.headers.update({
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.lamborghini-tractors.com/en-eu/dealer-locator",
    })

    base_url = "https://www.lamborghini-tractors.com/index.php"

    # Search from multiple center points to ensure full global coverage
    search_points = [
        ("Europe Central", 48.0, 10.0),
        ("Americas", 20.0, -80.0),
        ("Asia", 30.0, 100.0),
        ("Africa", 0.0, 25.0),
        ("Oceania", -25.0, 135.0),
    ]

    all_dealers = {}

    for name, lat, lng in search_points:
        print(f"  Searching from {name} ({lat}, {lng})...")
        try:
            resp = session.get(
                base_url,
                params={
                    "option": "com_dealerlocator",
                    "task": "search.searchDealer",
                    "lang": "en-EU",
                    "lat": str(lat),
                    "lng": str(lng),
                    "range": "50000",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("success") == 1 and data.get("dealers"):
                for dealer in data["dealers"]:
                    dealer_id = dealer.get("hda_id") or dealer.get("dealer_id") or str(dealer.get("id", ""))
                    if dealer_id not in all_dealers:
                        all_dealers[dealer_id] = dealer

                print(f"    Found {len(data['dealers'])} dealers, "
                      f"total unique: {len(all_dealers)}")
            else:
                print(f"    No dealers in response")

        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(1)

    print(f"\n  Total unique dealers from Lamborghini API: {len(all_dealers)}")

    # Convert to standardized format
    records = []
    for dealer_id, d in all_dealers.items():
        # Extract brands
        brands_list = d.get("brands", [])
        brand_str = "; ".join(brands_list) if brands_list else "SDF Group"

        # Extract services
        services = d.get("services", [])
        service_names = []
        for svc in services:
            if isinstance(svc, dict):
                service_names.append(svc.get("value", svc.get("label", "")))
            else:
                service_names.append(str(svc))
        services_str = "; ".join(service_names) if service_names else ""

        records.append({
            "brand": brand_str,
            "dealer_name": d.get("name", "").strip(),
            "address": d.get("address", "").strip(),
            "city": d.get("city", "").strip(),
            "state_region": d.get("state", d.get("prov", "")).strip(),
            "country": d.get("country", "").strip(),
            "postal_code": d.get("zip", "").strip(),
            "latitude": d.get("lat", "").strip(),
            "longitude": d.get("lng", "").strip(),
            "phone": d.get("phone", "").strip(),
            "fax": d.get("fax", "").strip(),
            "email": d.get("email", "").strip(),
            "website": d.get("website", "").strip(),
            "dealer_type": d.get("dealer_type", "").strip(),
            "services_offered": services_str,
            "dealer_id": dealer_id,
            "sap_code": d.get("sap_code", "").strip(),
            "source": "lamborghini_api",
        })

    return records


def scrape_same_nuxt_pages():
    """
    Scrape SAME dealer data from SSR-rendered Nuxt pages across all locales.
    Each locale page has dealers embedded in the __NUXT__ state.
    """
    print("\n" + "=" * 60)
    print("Scraping SAME Nuxt SSR pages...")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(HEADERS)

    # SAME locale URLs
    same_urls = {
        "en-gb": "https://www.same-tractors.com/en-gb/dealer-locator",
        "de-de": "https://www.same-tractors.com/de-de/vertragshaendler-suche",
        "it-it": "https://www.same-tractors.com/it-it/dealer-locator",
        "fr-fr": "https://www.same-tractors.com/fr-fr/chercher-concessionnaire",
        "es-es": "https://www.same-tractors.com/es-es/buscar-concesionario",
        "en-eu": "https://www.same-tractors.com/en-eu/dealer-locator",
        "pt-pt": "https://www.same-tractors.com/pt-pt/procurar-concessionario",
        "de-ce": "https://www.same-tractors.com/de-ce/vertragshaendler-suche",
        "de-ch": "https://www.same-tractors.com/de-ch/vertragshaendler-suche",
        "tr-tr": "https://www.same-tractors.com/tr-tr/bayi-bul",
        "en-za": "https://www.same-tractors.com/en-za/dealer-locator",
        "en-ea": "https://www.same-tractors.com/en-ea/dealer-locator",
        "en-fe": "https://www.same-tractors.com/en-fe/dealer-locator",
        "es-la": "https://www.same-tractors.com/es-la/buscar-concesionario",
    }

    # Map locale to likely country
    locale_country_map = {
        "en-gb": "GB",
        "de-de": "DE",
        "it-it": "IT",
        "fr-fr": "FR",
        "es-es": "ES",
        "en-eu": "EU",  # multiple countries
        "pt-pt": "PT",
        "de-ce": "AT",  # Central Europe (AT/CZ/HU)
        "de-ch": "CH",
        "tr-tr": "TR",
        "en-za": "ZA",
        "en-ea": "EA",  # East Asia
        "en-fe": "FE",  # Far East
        "es-la": "LA",  # Latin America
    }

    all_stores = {}

    for locale, url in same_urls.items():
        print(f"  Fetching {locale}...")
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code != 200:
                print(f"    HTTP {resp.status_code}")
                continue

            content = resp.text

            # Extract store data from __NUXT__ state
            # Pattern: {id:XXXX,name:"...",distance:...,address:"...",services:[...],latitude:"...",longitude:"...",phone:"...",fax:...,website:"..."}
            # Services use variable references like i,h,f which map to PARTS,SERVICE,TRACTORS respectively

            # First extract the variable mapping from the NUXT function args
            var_map = _extract_var_map(content)

            # Extract stores from the storelocator state
            # The store objects may contain variable references for any field
            # Pattern: {id:XXXX,name:"...",distance:VAR,address:"...",services:[...],
            #           latitude:"...",longitude:"...",phone:VAR_OR_STRING,fax:VAR,website:VAR_OR_STRING}
            stores = _extract_nuxt_stores(content, var_map)

            for store in stores:
                key = f"same_nuxt_{store['id']}"
                if key not in all_stores:
                    all_stores[key] = {
                        **store,
                        "locale": locale,
                        "country_hint": locale_country_map.get(locale, ""),
                    }

            print(f"    Found {len(stores)} stores")

        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(1.5)

    print(f"\n  Total unique stores from SAME Nuxt: {len(all_stores)}")

    # Convert to standardized format
    records = []
    for key, s in all_stores.items():
        services_str = "; ".join(s.get("services", []))

        # Parse address for city/postal
        city, postal = _parse_address(s["address"])

        records.append({
            "brand": "SAME",
            "dealer_name": s["name"],
            "address": s["address"],
            "city": city,
            "state_region": "",
            "country": s["country_hint"],
            "postal_code": postal,
            "latitude": s["latitude"],
            "longitude": s["longitude"],
            "phone": s["phone"],
            "fax": "",
            "email": "",
            "website": s["website"],
            "dealer_type": "",
            "services_offered": services_str,
            "dealer_id": s["id"],
            "sap_code": "",
            "source": f"same_nuxt_{s['locale']}",
        })

    return records


def _extract_var_map(html_content):
    """
    Extract the variable-to-value mapping from the NUXT function args.
    The NUXT state is: __NUXT__=(function(a,b,c,...){...}(val_a,val_b,...))
    Returns a dict mapping variable names to their resolved values.
    """
    var_map = {}

    nuxt_match = re.search(
        r'__NUXT__=\(function\(([^)]+)\)\{.*?\}\(([^)]+)\)\)',
        html_content,
        re.DOTALL
    )
    if not nuxt_match:
        return var_map

    params = [p.strip() for p in nuxt_match.group(1).split(",")]
    args_raw = nuxt_match.group(2)

    # Parse the arguments string - handle quoted strings and nested structures
    args = _parse_js_args(args_raw)

    for param, val in zip(params, args):
        # Remove quotes for string values
        cleaned = val.strip()
        if (cleaned.startswith('"') and cleaned.endswith('"')) or \
           (cleaned.startswith("'") and cleaned.endswith("'")):
            cleaned = cleaned[1:-1]
            # Unescape common JS escapes
            cleaned = cleaned.replace("\\u002F", "/")
        var_map[param] = cleaned

    return var_map


def _parse_js_args(args_raw):
    """Parse a comma-separated JS argument list, respecting strings and nesting."""
    args = []
    i = 0
    current = ""
    depth = 0
    in_string = False
    string_char = None

    while i < len(args_raw):
        ch = args_raw[i]

        if in_string:
            if ch == "\\" and i + 1 < len(args_raw):
                current += ch + args_raw[i + 1]
                i += 2
                continue
            elif ch == string_char:
                in_string = False
                current += ch
            else:
                current += ch
        else:
            if ch in ('"', "'"):
                in_string = True
                string_char = ch
                current += ch
            elif ch in ("{", "["):
                depth += 1
                current += ch
            elif ch in ("}", "]"):
                depth -= 1
                current += ch
            elif ch == "," and depth == 0:
                args.append(current.strip())
                current = ""
                i += 1
                continue
            else:
                current += ch
        i += 1

    if current.strip():
        args.append(current.strip())

    return args


def _extract_nuxt_stores(html_content, var_map):
    """
    Extract store objects from the __NUXT__ inline state.
    Handles variable references by resolving them through var_map.
    """
    stores = []

    # Find each store object starting with {id:XXXX,name:"
    store_starts = list(re.finditer(r'\{id:(\d+),name:"([^"]*)"', html_content))

    for match in store_starts:
        store_id = match.group(1)
        store_name = match.group(2)

        # Extract the full object by finding matching braces
        start = match.start()
        depth = 0
        end = start
        for i in range(start, min(start + 2000, len(html_content))):
            if html_content[i] == '{':
                depth += 1
            elif html_content[i] == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        obj_str = html_content[start:end]

        # Parse fields from the object string
        address = _extract_field(obj_str, "address", var_map)
        latitude = _extract_field(obj_str, "latitude", var_map)
        longitude = _extract_field(obj_str, "longitude", var_map)
        phone = _extract_field(obj_str, "phone", var_map)
        website = _extract_field(obj_str, "website", var_map)

        # Extract services array
        services_match = re.search(r'services:\[([^\]]*)\]', obj_str)
        services = []
        if services_match:
            svc_raw = services_match.group(1)
            for sv in svc_raw.split(","):
                sv = sv.strip().strip('"').strip("'")
                if sv:
                    resolved = var_map.get(sv, sv)
                    services.append(resolved)

        stores.append({
            "id": store_id,
            "name": store_name,
            "address": address,
            "services": services,
            "latitude": latitude,
            "longitude": longitude,
            "phone": phone,
            "website": website,
        })

    return stores


def _extract_field(obj_str, field_name, var_map):
    """
    Extract a field value from a JS object string, resolving variable references.
    Handles both quoted strings and variable references.
    """
    # Try quoted string first: field:"value"
    pattern = field_name + r':"([^"]*)"'
    match = re.search(pattern, obj_str)
    if match:
        val = match.group(1)
        # Unescape common JS escapes
        val = val.replace("\\u002F", "/")
        return val

    # Try variable reference: field:varName
    # Variable names are typically single chars or short identifiers
    pattern = field_name + r':([a-zA-Z_$][a-zA-Z0-9_$]*)'
    match = re.search(pattern, obj_str)
    if match:
        var_name = match.group(1)
        # Special JS values
        if var_name in ("null", "undefined", "true", "false"):
            return "" if var_name in ("null", "undefined") else var_name
        resolved = var_map.get(var_name, "")
        return resolved

    # Try numeric: field:123
    pattern = field_name + r':(\d+(?:\.\d+)?)'
    match = re.search(pattern, obj_str)
    if match:
        return match.group(1)

    return ""


def _parse_address(address):
    """Try to extract city and postal code from an address string."""
    city = ""
    postal = ""

    # Try to find postal code pattern
    postal_match = re.search(r'(\b[A-Z]?\d{4,5}\b|\b[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}\b)', address)
    if postal_match:
        postal = postal_match.group(1)

    # City is often the last major component
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        # Last part might be country or city+postal
        last = parts[-1].strip()
        # Remove postal code from last part to get city
        city_candidate = re.sub(r'\b[A-Z]?\d{4,5}\b|\b[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}\b', '', last).strip()
        if city_candidate:
            city = city_candidate

    return city, postal


def merge_and_deduplicate(lamborghini_records, same_records):
    """
    Merge records from both sources and deduplicate.
    Lamborghini API records are preferred (more structured data).
    SAME Nuxt records fill in gaps.
    """
    print("\n" + "=" * 60)
    print("Merging and deduplicating...")
    print("=" * 60)

    # Build index by (lat, lng) rounded for fuzzy matching
    seen = {}
    final = []

    # Add Lamborghini records first (higher quality)
    for r in lamborghini_records:
        key = _dedup_key(r)
        if key not in seen:
            seen[key] = r
            final.append(r)

    added_from_same = 0
    for r in same_records:
        key = _dedup_key(r)
        if key not in seen:
            # Also check by name similarity
            name_key = r["dealer_name"].upper().strip()
            name_match = False
            for existing in final:
                if existing["dealer_name"].upper().strip() == name_key:
                    name_match = True
                    break
            if not name_match:
                seen[key] = r
                final.append(r)
                added_from_same += 1

    print(f"  Lamborghini API records: {len(lamborghini_records)}")
    print(f"  SAME Nuxt records: {len(same_records)}")
    print(f"  Added from SAME (not in Lamborghini): {added_from_same}")
    print(f"  Final deduplicated count: {len(final)}")

    return final


def _dedup_key(record):
    """Generate a deduplication key from lat/lng rounded to ~100m."""
    try:
        lat = round(float(record["latitude"]), 3)
        lng = round(float(record["longitude"]), 3)
        return (lat, lng)
    except (ValueError, TypeError):
        # Fallback to name-based key
        return ("name", record["dealer_name"].upper().strip())


def write_csv(records, output_path):
    """Write records to CSV."""
    print(f"\nWriting {len(records)} records to {output_path}")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in sorted(records, key=lambda x: (x["country"], x["dealer_name"])):
            writer.writerow(r)

    print(f"  Done: {output_path}")


def print_summary(records):
    """Print a summary of the scraped data."""
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    # By country
    by_country = {}
    for r in records:
        c = r["country"] or "Unknown"
        by_country[c] = by_country.get(c, 0) + 1

    print(f"\nTotal dealers: {len(records)}")
    print(f"Countries: {len(by_country)}")
    print("\nBy country:")
    for country, count in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {country}: {count}")

    # By brand
    brands = {}
    for r in records:
        for b in r["brand"].split("; "):
            b = b.strip()
            if b:
                brands[b] = brands.get(b, 0) + 1

    print("\nBy brand:")
    for brand, count in sorted(brands.items(), key=lambda x: -x[1]):
        print(f"  {brand}: {count}")

    # By source
    by_source = {}
    for r in records:
        s = r["source"]
        by_source[s] = by_source.get(s, 0) + 1
    print("\nBy source:")
    for src, count in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"  {src}: {count}")


def main():
    lamborghini_records = scrape_lamborghini_api()
    same_records = scrape_same_nuxt_pages()
    all_records = merge_and_deduplicate(lamborghini_records, same_records)
    write_csv(all_records, OUTPUT_CSV)
    print_summary(all_records)


if __name__ == "__main__":
    main()
