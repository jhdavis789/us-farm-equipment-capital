#!/usr/bin/env python3
"""
Scrape Argo Tractors dealer/service locations.
Brands: McCormick, Landini.

Data source: dealers.argotractors.com WordPress site
Dealer data is embedded in HTML as a JSON data-markers attribute on the Google Maps element.
Each locale/brand combination has its own page with potentially different dealers.
"""

import requests
import re
import json
import csv
import time
import html as html_mod
from pathlib import Path

# Output paths
SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR.parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

OUTPUT_CSV = RAW_DIR / "argo_dealers.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CSV_FIELDS = [
    "brand", "dealer_name", "address", "city", "state_region", "country",
    "postal_code", "latitude", "longitude", "phone", "fax", "email",
    "website", "dealer_type", "services_offered", "dealer_id", "source"
]

# Argo locale -> country/region mapping
LOCALE_COUNTRY_MAP = {
    "it": "IT",
    "en": "GB",
    "fr": "FR",
    "pl": "PL",
    "es": "ES",
    "mx": "MX",
    "pt-pt": "PT",
    "za": "ZA",
    "de": "DE",
    "cs": "CZ",
    "tr": "TR",
    "as": "INTL",   # International
    "es-ar": "AR",
    "au": "AU",
    "nz": "NZ",
    "us": "US",
    "ie": "IE",
    "pt-br": "BR",
}


def extract_dealers_from_page(html_content):
    """Extract dealer JSON data from the data-markers attribute in the HTML."""
    match = re.search(r'data-markers="([^"]+)"', html_content)
    if not match:
        return []

    raw_json = html_mod.unescape(match.group(1))
    try:
        data = json.loads(raw_json)
        if not isinstance(data, list):
            data = [data]
        return data
    except json.JSONDecodeError:
        return []


def parse_dealer(dealer_post, brand, locale):
    """
    Parse a single dealer post (which may have multiple sub-dealers/locations)
    into standardized records.
    """
    records = []
    post_id = dealer_post.get("ID", "")
    post_title = dealer_post.get("post_title", "").strip()
    external_website = dealer_post.get("external_website", "").strip()

    sub_dealers = dealer_post.get("dealers", [])
    if not sub_dealers or not isinstance(sub_dealers, list):
        return records

    for idx, sub in enumerate(sub_dealers):
        if not isinstance(sub, dict):
            continue
        addr_obj = sub.get("address", {})
        if not isinstance(addr_obj, dict):
            addr_obj = {}
        address_str = addr_obj.get("address", "").strip() if isinstance(addr_obj.get("address"), str) else ""
        lat = addr_obj.get("lat", 0) if isinstance(addr_obj.get("lat"), (int, float)) else 0
        lng = addr_obj.get("lng", 0) if isinstance(addr_obj.get("lng"), (int, float)) else 0

        # Parse address components
        city, postal, country, state_region = _parse_argo_address(
            address_str, LOCALE_COUNTRY_MAP.get(locale, "")
        )

        # Sub-dealer name might be empty, use post title
        name = sub.get("name", "").strip() or post_title

        records.append({
            "brand": brand,
            "dealer_name": name,
            "address": address_str,
            "city": city,
            "state_region": state_region,
            "country": country,
            "postal_code": postal,
            "latitude": str(lat) if lat else "",
            "longitude": str(lng) if lng else "",
            "phone": sub.get("phone", "").strip(),
            "fax": sub.get("fax", "").strip(),
            "email": sub.get("email", "").strip(),
            "website": sub.get("website", "").strip() or external_website,
            "dealer_type": "",
            "services_offered": "",
            "dealer_id": f"{post_id}.{idx}" if len(sub_dealers) > 1 else str(post_id),
            "source": f"argo_{locale}",
        })

    return records


def _parse_argo_address(address, country_hint):
    """
    Parse Argo address string into components.
    Typical formats:
      ", POSTAL City, Country"
      ", POSTAL City, State"
      "Street, POSTAL City, Country"
      "Street, City State POSTAL, Country"
    """
    city = ""
    postal = ""
    country = country_hint
    state_region = ""

    if not address:
        return city, postal, country, state_region

    parts = [p.strip() for p in address.split(",")]
    # Remove empty leading parts (common pattern: ", POSTAL City, ...")
    while parts and not parts[0]:
        parts.pop(0)

    if not parts:
        return city, postal, country, state_region

    # Check if last part is a country name
    country_names = {
        "italy": "IT", "italia": "IT", "germany": "DE", "deutschland": "DE",
        "france": "FR", "spain": "ES", "espana": "ES", "espa\u00f1a": "ES",
        "united kingdom": "GB", "uk": "GB", "great britain": "GB",
        "ireland": "IE", "portugal": "PT", "netherlands": "NL",
        "belgium": "BE", "austria": "AT", "switzerland": "CH",
        "poland": "PL", "czech republic": "CZ", "czechia": "CZ",
        "hungary": "HU", "romania": "RO", "bulgaria": "BG",
        "croatia": "HR", "serbia": "RS", "slovenia": "SI",
        "slovakia": "SK", "greece": "GR", "turkey": "TR", "t\u00fcrkiye": "TR",
        "sweden": "SE", "norway": "NO", "denmark": "DK",
        "finland": "FI", "estonia": "EE", "latvia": "LV",
        "lithuania": "LT", "south africa": "ZA", "australia": "AU",
        "new zealand": "NZ", "united states": "US", "usa": "US",
        "mexico": "MX", "m\u00e9xico": "MX", "argentina": "AR", "brazil": "BR", "brasil": "BR",
        "chile": "CL", "colombia": "CO", "peru": "PE",
        "kingdom of saudi arabia": "SA", "saudi arabia": "SA",
        "oman": "OM", "qatar": "QA", "uae": "AE",
        "japan": "JP", "china": "CN", "india": "IN",
        "thailand": "TH", "malaysia": "MY", "singapore": "SG",
        "indonesia": "ID", "philippines": "PH", "vietnam": "VN",
        "kenya": "KE", "nigeria": "NG", "ghana": "GH",
        "tanzania": "TZ", "uganda": "UG", "zambia": "ZM",
        "zimbabwe": "ZW", "mozambique": "MZ", "madagascar": "MG",
        "malawi": "MW", "botswana": "BW", "namibia": "NA",
            "costa rica": "CR", "guatemala": "GT", "honduras": "HN",
            "nicaragua": "NI", "el salvador": "SV", "panama": "PA",
            "dominican republic": "DO", "jamaica": "JM", "trinidad and tobago": "TT",
            "uruguay": "UY", "paraguay": "PY", "bolivia": "BO",
            "venezuela": "VE", "ecuador": "EC",
        }
    # Check last part(s) against country names, stripping whitespace
    for i in range(len(parts) - 1, max(len(parts) - 3, -1), -1):
        candidate = parts[i].strip().lower()
        if candidate in country_names:
            country = country_names[candidate]
            parts = parts[:i]
            break

    # Now find the part with postal code + city
    # Common formats:
    #   "POSTAL City" (European: "4343 Gatton")
    #   "City State POSTAL" (US: "Norridgewock ME 04957")
    #   "City POSTAL" (UK: "BARNSTAPLE EX32 9BA")
    #   just "City" or "State"
    for idx in range(len(parts) - 1, -1, -1):
        segment = parts[idx].strip()
        if not segment:
            continue

        # Try pattern: leading digits (European postal code)
        postal_match = re.match(
            r'^(\d{4,6})\s+(.*)', segment
        )
        if postal_match:
            postal = postal_match.group(1).strip()
            city = postal_match.group(2).strip()
            # Check if there's a state in the remaining parts
            if idx + 1 < len(parts):
                state_region = parts[idx + 1].strip()
            break

        # Try pattern: digits with dash (e.g., "12345-678")
        postal_match = re.match(
            r'^(\d{5}-\d{3})\s+(.*)', segment
        )
        if postal_match:
            postal = postal_match.group(1).strip()
            city = postal_match.group(2).strip()
            break

        # Try pattern: UK postcode (e.g., "City EX32 9BA" or "EX32 9BA City")
        uk_match = re.search(
            r'([A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2})', segment
        )
        if uk_match:
            postal = uk_match.group(1)
            city = segment.replace(postal, '').strip()
            break

        # Try pattern: German/Austrian (e.g., "D-12345 City")
        de_match = re.match(r'^[A-Z]-?(\d{4,5})\s+(.*)', segment)
        if de_match:
            postal = de_match.group(1).strip()
            city = de_match.group(2).strip()
            break

        # Try US format: "City, ST XXXXX" or just "ST" as state
        us_match = re.match(r'^(\d{5}(?:-\d{4})?)\s+(.*)', segment)
        if us_match:
            postal = us_match.group(1).strip()
            city = us_match.group(2).strip()
            break

        # If we're at the last part and nothing matched, it's likely just a city
        if idx == len(parts) - 1:
            # Check for trailing postal code
            trail_match = re.search(r'\s+(\d{4,6})$', segment)
            if trail_match:
                postal = trail_match.group(1)
                city = segment[:trail_match.start()].strip()
            else:
                city = segment
            break

    # If city is still empty but we have parts, use the last non-empty part
    if not city and parts:
        for p in reversed(parts):
            p = p.strip()
            if p:
                city = p
                break

    return city, postal, country, state_region


def scrape_brand(brand, locales, session):
    """Scrape all locales for a given brand."""
    print(f"\n{'=' * 60}")
    print(f"Scraping {brand.upper()} dealers...")
    print(f"{'=' * 60}")

    all_records = {}

    for locale in locales:
        url = f"https://dealers.argotractors.com/{locale}/{brand}/"
        print(f"  Fetching {locale}...")

        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 404:
                print(f"    404 Not Found")
                continue
            if resp.status_code != 200:
                print(f"    HTTP {resp.status_code}")
                continue

            if not resp.text or len(resp.text) < 100:
                print(f"    Empty response")
                continue

            dealers = extract_dealers_from_page(resp.text)
            if not dealers:
                print(f"    No dealer data found")
                continue

            locale_records = 0
            for dealer in dealers:
                records = parse_dealer(dealer, brand.capitalize(), locale)
                for r in records:
                    # Deduplicate by dealer_id
                    key = f"{brand}_{dealer.get('ID', '')}_{r['latitude']}_{r['longitude']}"
                    if key not in all_records:
                        all_records[key] = r
                        locale_records += 1

            print(f"    Found {len(dealers)} dealer posts, "
                  f"{locale_records} new sub-locations, "
                  f"total unique: {len(all_records)}")

        except Exception as e:
            print(f"    Error: {e}")

        time.sleep(1.5)

    print(f"\n  Total unique {brand} locations: {len(all_records)}")
    return list(all_records.values())


def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    all_locales = list(LOCALE_COUNTRY_MAP.keys())

    mccormick_records = scrape_brand("mccormick", all_locales, session)
    landini_records = scrape_brand("landini", all_locales, session)

    all_records = mccormick_records + landini_records

    # Final deduplication by (lat, lng, brand)
    seen = {}
    final = []
    for r in all_records:
        try:
            lat = round(float(r["latitude"]), 4)
            lng = round(float(r["longitude"]), 4)
            key = (r["brand"].lower(), lat, lng)
        except (ValueError, TypeError):
            key = (r["brand"].lower(), r["dealer_name"].upper())

        if key not in seen:
            seen[key] = True
            final.append(r)

    print(f"\n{'=' * 60}")
    print(f"FINAL RESULTS")
    print(f"{'=' * 60}")
    print(f"McCormick records: {len(mccormick_records)}")
    print(f"Landini records: {len(landini_records)}")
    print(f"After dedup: {len(final)}")

    # Write CSV
    print(f"\nWriting {len(final)} records to {OUTPUT_CSV}")
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in sorted(final, key=lambda x: (x["brand"], x["country"], x["dealer_name"])):
            writer.writerow(r)
    print(f"  Done: {OUTPUT_CSV}")

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")

    by_brand = {}
    for r in final:
        by_brand[r["brand"]] = by_brand.get(r["brand"], 0) + 1
    print("\nBy brand:")
    for brand, count in sorted(by_brand.items()):
        print(f"  {brand}: {count}")

    by_country = {}
    for r in final:
        c = r["country"] or "Unknown"
        by_country[c] = by_country.get(c, 0) + 1
    print(f"\nCountries: {len(by_country)}")
    print("By country:")
    for country, count in sorted(by_country.items(), key=lambda x: -x[1]):
        print(f"  {country}: {count}")


if __name__ == "__main__":
    main()
