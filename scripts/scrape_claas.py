#!/usr/bin/env python3
"""
Scrape all CLAAS dealer/service locations across Europe and the USA.

Uses the CLAAS dealer locator API:
  - Token endpoint: https://www.claas.com/api/service/dealerlocator/token
  - Search endpoint: https://ext-projects.connect.claas.com/v2/dealers/search
  - Filters endpoint: https://ext-projects.connect.claas.com/v2/dealer-filters

The search API requires a country code and returns all dealers for that country
when queried with a space character as the search term.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# --- Configuration ---

TOKEN_URL = "https://www.claas.com/api/service/dealerlocator/token"
SEARCH_URL = "https://ext-projects.connect.claas.com/v2/dealers/search"
FILTERS_URL = "https://ext-projects.connect.claas.com/v2/dealer-filters"

DEALER_LOCATOR_ID = "12"
LANGUAGE = "en"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Countries to scrape: USA + all European countries with potential CLAAS presence
COUNTRIES = {
    # North America
    "US": "United States",
    # Western Europe
    "DE": "Germany",
    "FR": "France",
    "GB": "United Kingdom",
    "NL": "Netherlands",
    "BE": "Belgium",
    "LU": "Luxembourg",
    "AT": "Austria",
    "CH": "Switzerland",
    "IE": "Ireland",
    # Southern Europe
    "IT": "Italy",
    "ES": "Spain",
    "PT": "Portugal",
    "GR": "Greece",
    "MT": "Malta",
    "CY": "Cyprus",
    # Northern Europe
    "SE": "Sweden",
    "FI": "Finland",
    "DK": "Denmark",
    "NO": "Norway",
    "IS": "Iceland",
    # Central/Eastern Europe
    "PL": "Poland",
    "CZ": "Czech Republic",
    "SK": "Slovakia",
    "HU": "Hungary",
    "RO": "Romania",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "SI": "Slovenia",
    "RS": "Serbia",
    "BA": "Bosnia and Herzegovina",
    "ME": "Montenegro",
    "MK": "North Macedonia",
    "AL": "Albania",
    "XK": "Kosovo",
    # Baltic States
    "LT": "Lithuania",
    "LV": "Latvia",
    "EE": "Estonia",
    # Other European
    "UA": "Ukraine",
    "MD": "Moldova",
    "BY": "Belarus",
    "TR": "Turkey",
    "GE": "Georgia",
}

# Delivery program icon to human-readable mapping
DELIVERY_PROGRAM_MAP = {
    "tractors": "Tractors",
    "combines": "Combines",
    "forage_harvester": "Forage Harvesters",
    "forage_harvester_machinery": "Forage Harvesting Machinery",
    "balers": "Balers",
    "telehandlers": "Telehandlers",
    "service_and_parts": "Service & Parts",
    "easy": "EASY (Electronics)",
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dealer_data", "raw")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "claas_dealers.csv")


def get_token(session: requests.Session) -> str:
    """Fetch a bearer token from the CLAAS token endpoint."""
    resp = session.get(TOKEN_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return f"{data['token_type']} {data['access_token']}"


def search_dealers_by_country(session: requests.Session, auth_header: str, country_code: str) -> list:
    """
    Search for all CLAAS dealers in a given country.
    Returns a list of dealer dicts.
    """
    search_headers = {**HEADERS, "Authorization": auth_header}
    payload = {
        "query": " ",
        "country": country_code,
        "filters": [],
    }
    params = {
        "dealerLocatorId": DEALER_LOCATOR_ID,
        "language": LANGUAGE,
    }

    resp = session.post(SEARCH_URL, headers=search_headers, params=params, json=payload, timeout=30)

    if resp.status_code == 400:
        # Country may not have dealers
        return []
    resp.raise_for_status()

    data = resp.json()
    return data.get("content", [])


def _safe_str(val) -> str:
    """Safely convert a value to a stripped string, handling None."""
    if val is None:
        return ""
    return str(val).strip()


def parse_dealer(dealer: dict, country_name: str) -> dict:
    """Parse a raw dealer API response into a flat CSV row."""
    delivery_icons = dealer.get("deliveryProgramIcons") or []
    services = [DELIVERY_PROGRAM_MAP.get(icon, icon) for icon in delivery_icons]

    # Determine dealer type based on delivery programs
    has_sales = any(
        icon in delivery_icons
        for icon in ["tractors", "combines", "forage_harvester", "balers", "telehandlers"]
    )
    has_service = "service_and_parts" in delivery_icons
    if has_sales and has_service:
        dealer_type = "Sales & Service"
    elif has_sales:
        dealer_type = "Sales"
    elif has_service:
        dealer_type = "Service Only"
    else:
        dealer_type = "Other"

    return {
        "brand": "CLAAS",
        "dealer_id": _safe_str(dealer.get("dealerId")),
        "dealer_name": _safe_str(dealer.get("name")),
        "name_affix": _safe_str(dealer.get("nameAffix")),
        "address": _safe_str(dealer.get("street")),
        "house_number": _safe_str(dealer.get("houseNo")),
        "city": _safe_str(dealer.get("city")),
        "state_region": _safe_str(dealer.get("state")),
        "country": _safe_str(dealer.get("country")),
        "country_name": country_name,
        "postal_code": _safe_str(dealer.get("postalCode")),
        "latitude": dealer.get("latitude") if dealer.get("latitude") is not None else "",
        "longitude": dealer.get("longitude") if dealer.get("longitude") is not None else "",
        "phone": _safe_str(dealer.get("phone")),
        "email": _safe_str(dealer.get("email")),
        "website": _safe_str(dealer.get("url")),
        "dealer_type": dealer_type,
        "is_independent_service_partner": dealer.get("independentServicePartner", False),
        "services_offered": "; ".join(services),
        "delivery_programs": "; ".join(dealer.get("deliveryPrograms") or []),
        "delivery_program_icons": "; ".join(delivery_icons),
        "dealer_locator_ids": "; ".join(dealer.get("dealerLocators") or []),
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    session = requests.Session()

    # Get auth token
    print("Fetching auth token...")
    auth_header = get_token(session)
    print("Token acquired.")

    all_dealers = []
    seen_ids = set()
    country_stats = {}

    total_countries = len(COUNTRIES)
    for i, (country_code, country_name) in enumerate(sorted(COUNTRIES.items()), 1):
        print(f"[{i:2d}/{total_countries}] Searching {country_name} ({country_code})...", end=" ", flush=True)

        try:
            dealers = search_dealers_by_country(session, auth_header, country_code)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                # Token expired, refresh
                print("(token refresh)...", end=" ", flush=True)
                auth_header = get_token(session)
                dealers = search_dealers_by_country(session, auth_header, country_code)
            else:
                print(f"ERROR: {e}")
                country_stats[country_code] = 0
                continue

        new_count = 0
        dup_count = 0
        for dealer in dealers:
            did = dealer.get("dealerId", "")
            # Deduplicate by (dealerId, country) since same dealer can appear in multiple countries
            dedup_key = (did, dealer.get("country", country_code))
            if dedup_key in seen_ids:
                dup_count += 1
                continue
            seen_ids.add(dedup_key)

            parsed = parse_dealer(dealer, country_name)
            all_dealers.append(parsed)
            new_count += 1

        country_stats[country_code] = new_count
        dup_msg = f" ({dup_count} duplicates skipped)" if dup_count > 0 else ""
        print(f"{len(dealers)} found, {new_count} new{dup_msg}")

        # Be polite
        time.sleep(0.3)

    # Sort by country, then dealer name
    all_dealers.sort(key=lambda d: (d["country"], d["dealer_name"], d["city"]))

    # Write CSV
    if not all_dealers:
        print("No dealers found!")
        sys.exit(1)

    fieldnames = [
        "brand",
        "dealer_id",
        "dealer_name",
        "name_affix",
        "address",
        "house_number",
        "city",
        "state_region",
        "country",
        "country_name",
        "postal_code",
        "latitude",
        "longitude",
        "phone",
        "email",
        "website",
        "dealer_type",
        "is_independent_service_partner",
        "services_offered",
        "delivery_programs",
        "delivery_program_icons",
        "dealer_locator_ids",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_dealers)

    # Print summary
    print(f"\n{'='*60}")
    print(f"CLAAS Dealer Scrape Complete")
    print(f"{'='*60}")
    print(f"Total unique dealers: {len(all_dealers)}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Timestamp: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()

    # Country breakdown
    print("Dealers by country:")
    for code in sorted(country_stats, key=lambda c: -country_stats[c]):
        count = country_stats[code]
        if count > 0:
            print(f"  {COUNTRIES[code]:30s} ({code}): {count:4d}")

    # Count by dealer type
    type_counts = {}
    for d in all_dealers:
        dt = d["dealer_type"]
        type_counts[dt] = type_counts.get(dt, 0) + 1
    print()
    print("By dealer type:")
    for dt, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {dt:25s}: {count:4d}")

    # Count dealers with tractors in delivery programs
    tractor_count = sum(
        1 for d in all_dealers if "tractors" in d["delivery_program_icons"].lower()
    )
    print(f"\nDealers handling tractors: {tractor_count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
