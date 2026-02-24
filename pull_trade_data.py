#!/usr/bin/env python3
"""
Pull US import/export data for farm equipment from Census Bureau API.

HS 8701: Tractors
  - Pre-2017: 870190 = "Tractors, NESOI" (includes agricultural)
    - HS10: 8701901001-8701901090 = agricultural use tractors
    - HS10: 8701905015-8701905025 = other tractors NESOI
  - Post-2017: 870191-870195 = tractors by engine power
    - HS10: 8701911000, 8701921000, 8701931000, 8701941000, 8701951000 = agricultural
    - HS10: 8701915000, 8701925000, 8701935000, 8701945000, 8701955000 = other NESOI

HS 8433: Harvesting/threshing machinery
  - 843351 = Combine harvester-threshers
    - HS10: 8433510010 = self-propelled combines
    - HS10: 8433510090 = other combines

Data from Census Bureau International Trade API:
  Imports: https://api.census.gov/data/timeseries/intltrade/imports/hs
  Exports: https://api.census.gov/data/timeseries/intltrade/exports/hs
"""

import json
import csv
import time
import urllib.request
import sys
from collections import defaultdict

IMPORT_BASE = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
EXPORT_BASE = "https://api.census.gov/data/timeseries/intltrade/exports/hs"

# Years to query
YEARS = list(range(2010, 2025))

def fetch_json(url, retries=3):
    """Fetch JSON from URL with retries."""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                return data
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt+1} for {url}: {e}", file=sys.stderr)
                time.sleep(2)
            else:
                print(f"  FAILED {url}: {e}", file=sys.stderr)
                return None
    return None


def pull_hs4_values(base_url, commodity_var, value_var, hs_code, years):
    """Pull HS4-level annual dollar values."""
    results = {}
    for year in years:
        url = (f"{base_url}?get={commodity_var},{value_var}"
               f"&COMM_LVL=HS4&{commodity_var}={hs_code}"
               f"&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            val = data[1][1]
            results[year] = int(val) if val and val != "0" else 0
        else:
            results[year] = None
        time.sleep(0.3)
    return results


def pull_hs6_values(base_url, commodity_var, value_var, hs6_codes, years):
    """Pull HS6-level annual dollar values for specific subcodes."""
    results = {}  # year -> total value
    for year in years:
        total = 0
        for hs6 in hs6_codes:
            url = (f"{base_url}?get={commodity_var},{value_var}"
                   f"&COMM_LVL=HS6&{commodity_var}={hs6}"
                   f"&YEAR={year}&MONTH=12")
            data = fetch_json(url)
            if data and len(data) > 1:
                val = data[1][1]
                if val and val != "0":
                    total += int(val)
            time.sleep(0.3)
        results[year] = total if total > 0 else None
    return results


def pull_hs10_quantities(base_url, commodity_var, value_var, qty_var, hs10_patterns, years):
    """Pull HS10-level annual quantities for specific patterns.
    Returns dict of year -> (total_value, total_quantity)
    """
    results = {}
    for year in years:
        total_val = 0
        total_qty = 0
        for pattern in hs10_patterns:
            url = (f"{base_url}?get={commodity_var},I_COMMODITY_LDESC,{value_var},{qty_var},UNIT_QY1"
                   f"&COMM_LVL=HS10&{commodity_var}={pattern}*"
                   f"&YEAR={year}&MONTH=12")
            if "exports" in base_url:
                url = (f"{base_url}?get={commodity_var},E_COMMODITY_LDESC,{value_var},{qty_var},UNIT_QY1"
                       f"&COMM_LVL=HS10&{commodity_var}={pattern}*"
                       f"&YEAR={year}&MONTH=12")
            data = fetch_json(url)
            if data and len(data) > 1:
                for row in data[1:]:
                    val = row[2]
                    qty = row[3]
                    if val and val != "0":
                        total_val += int(val)
                    if qty and qty != "0":
                        total_qty += int(qty)
            time.sleep(0.3)
        results[year] = (total_val if total_val > 0 else None,
                         total_qty if total_qty > 0 else None)
    return results


def pull_ag_tractor_imports(years):
    """Pull agricultural tractor import data (value + units).
    Pre-2017: HS10 870190xxxx codes for agricultural use
    Post-2017: HS10 87019[1-5]1000 codes for agricultural use
    """
    results = {}

    # Agricultural tractor HS10 patterns
    # Pre-2017 agricultural codes
    pre2017_ag_patterns = ["87019010"]  # covers 8701901001 through 8701901090
    # Post-2017 agricultural codes
    post2017_ag_patterns = ["87019110", "87019210", "87019310", "87019410", "87019510"]

    for year in years:
        total_val = 0
        total_qty = 0

        if year < 2017:
            patterns = pre2017_ag_patterns
        else:
            patterns = post2017_ag_patterns

        for pattern in patterns:
            url = (f"{IMPORT_BASE}?get=I_COMMODITY,I_COMMODITY_LDESC,CON_VAL_YR,CON_QY1_YR,UNIT_QY1"
                   f"&COMM_LVL=HS10&I_COMMODITY={pattern}*"
                   f"&YEAR={year}&MONTH=12")
            data = fetch_json(url)
            if data and len(data) > 1:
                for row in data[1:]:
                    val = row[2]
                    qty = row[3]
                    desc = row[1]
                    if val and val != "0":
                        total_val += int(val)
                    if qty and qty != "0":
                        total_qty += int(qty)
                    print(f"  {year} IMP {row[0]}: ${int(val):>15,} x {qty:>8} - {desc[:60]}", file=sys.stderr)
            time.sleep(0.3)

        results[year] = {
            "value": total_val if total_val > 0 else None,
            "units": total_qty if total_qty > 0 else None
        }
        print(f"  {year} AG TRACTOR IMPORTS: ${total_val:>15,}  units: {total_qty:>8,}", file=sys.stderr)

    return results


def pull_ag_tractor_exports(years):
    """Pull agricultural tractor export data."""
    results = {}

    # Export codes: same HS structure but using Schedule B / E_COMMODITY
    # Pre-2017: 870190xxxx
    # Post-2017: 87019[1-5]xxxx
    pre2017_patterns = ["87019010", "87019050"]  # ag + other
    post2017_patterns = ["87019110", "87019210", "87019310", "87019410", "87019510"]

    for year in years:
        total_val = 0
        total_qty = 0

        if year < 2017:
            patterns = pre2017_patterns
        else:
            patterns = post2017_patterns

        for pattern in patterns:
            url = (f"{EXPORT_BASE}?get=E_COMMODITY,E_COMMODITY_LDESC,ALL_VAL_YR,QTY_1_YR,UNIT_QY1"
                   f"&COMM_LVL=HS10&E_COMMODITY={pattern}*"
                   f"&YEAR={year}&MONTH=12")
            data = fetch_json(url)
            if data and len(data) > 1:
                for row in data[1:]:
                    val = row[2]
                    qty = row[3]
                    desc = row[1]
                    if val and val != "0":
                        total_val += int(val)
                    if qty and qty != "0":
                        total_qty += int(qty)
                    print(f"  {year} EXP {row[0]}: ${int(val):>15,} x {qty:>8} - {desc[:60]}", file=sys.stderr)
            time.sleep(0.3)

        results[year] = {
            "value": total_val if total_val > 0 else None,
            "units": total_qty if total_qty > 0 else None
        }
        print(f"  {year} AG TRACTOR EXPORTS: ${total_val:>15,}  units: {total_qty:>8,}", file=sys.stderr)

    return results


def pull_combine_imports(years):
    """Pull combine harvester-thresher import data."""
    results = {}

    for year in years:
        total_val = 0
        total_qty = 0

        url = (f"{IMPORT_BASE}?get=I_COMMODITY,I_COMMODITY_LDESC,CON_VAL_YR,CON_QY1_YR,UNIT_QY1"
               f"&COMM_LVL=HS10&I_COMMODITY=843351*"
               f"&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            for row in data[1:]:
                val = row[2]
                qty = row[3]
                desc = row[1]
                if val and val != "0":
                    total_val += int(val)
                if qty and qty != "0":
                    total_qty += int(qty)
                print(f"  {year} IMP {row[0]}: ${int(val):>15,} x {qty:>8} - {desc[:60]}", file=sys.stderr)
        time.sleep(0.3)

        results[year] = {
            "value": total_val if total_val > 0 else None,
            "units": total_qty if total_qty > 0 else None
        }
        print(f"  {year} COMBINE IMPORTS: ${total_val:>15,}  units: {total_qty:>8,}", file=sys.stderr)

    return results


def pull_combine_exports(years):
    """Pull combine harvester-thresher export data."""
    results = {}

    for year in years:
        total_val = 0
        total_qty = 0

        url = (f"{EXPORT_BASE}?get=E_COMMODITY,E_COMMODITY_LDESC,ALL_VAL_YR,QTY_1_YR,UNIT_QY1"
               f"&COMM_LVL=HS10&E_COMMODITY=843351*"
               f"&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            for row in data[1:]:
                val = row[2]
                qty = row[3]
                desc = row[1]
                if val and val != "0":
                    total_val += int(val)
                if qty and qty != "0":
                    total_qty += int(qty)
                print(f"  {year} EXP {row[0]}: ${int(val):>15,} x {qty:>8} - {desc[:60]}", file=sys.stderr)
        time.sleep(0.3)

        results[year] = {
            "value": total_val if total_val > 0 else None,
            "units": total_qty if total_qty > 0 else None
        }
        print(f"  {year} COMBINE EXPORTS: ${total_val:>15,}  units: {total_qty:>8,}", file=sys.stderr)

    return results


def pull_hs4_all_8701(years):
    """Pull ALL HS 8701 at HS4 level (all tractors including road tractors)."""
    imports = {}
    exports = {}
    for year in years:
        url = (f"{IMPORT_BASE}?get=I_COMMODITY,CON_VAL_YR"
               f"&COMM_LVL=HS4&I_COMMODITY=8701&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            imports[year] = int(data[1][1])
        else:
            imports[year] = None

        url = (f"{EXPORT_BASE}?get=E_COMMODITY,ALL_VAL_YR"
               f"&COMM_LVL=HS4&E_COMMODITY=8701&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            exports[year] = int(data[1][1])
        else:
            exports[year] = None
        time.sleep(0.3)
    return imports, exports


def pull_hs4_all_8433(years):
    """Pull ALL HS 8433 at HS4 level."""
    imports = {}
    exports = {}
    for year in years:
        url = (f"{IMPORT_BASE}?get=I_COMMODITY,CON_VAL_YR"
               f"&COMM_LVL=HS4&I_COMMODITY=8433&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            imports[year] = int(data[1][1])
        else:
            imports[year] = None

        url = (f"{EXPORT_BASE}?get=E_COMMODITY,ALL_VAL_YR"
               f"&COMM_LVL=HS4&E_COMMODITY=8433&YEAR={year}&MONTH=12")
        data = fetch_json(url)
        if data and len(data) > 1:
            exports[year] = int(data[1][1])
        else:
            exports[year] = None
        time.sleep(0.3)
    return imports, exports


def main():
    print("=" * 80, file=sys.stderr)
    print("Pulling US Farm Equipment Trade Data from Census Bureau API", file=sys.stderr)
    print("=" * 80, file=sys.stderr)

    # 1. HS4-level totals (all 8701 and all 8433)
    print("\n--- HS4 8701 (All Tractors) ---", file=sys.stderr)
    imp_8701_hs4, exp_8701_hs4 = pull_hs4_all_8701(YEARS)

    print("\n--- HS4 8433 (All Harvesting/Threshing) ---", file=sys.stderr)
    imp_8433_hs4, exp_8433_hs4 = pull_hs4_all_8433(YEARS)

    # 2. Agricultural tractors at HS10 level (with unit counts)
    print("\n--- Agricultural Tractor Imports (HS10) ---", file=sys.stderr)
    ag_tractor_imp = pull_ag_tractor_imports(YEARS)

    print("\n--- Agricultural Tractor Exports (HS10) ---", file=sys.stderr)
    ag_tractor_exp = pull_ag_tractor_exports(YEARS)

    # 3. Combines at HS10 level (with unit counts)
    print("\n--- Combine Imports (HS10) ---", file=sys.stderr)
    combine_imp = pull_combine_imports(YEARS)

    print("\n--- Combine Exports (HS10) ---", file=sys.stderr)
    combine_exp = pull_combine_exports(YEARS)

    # Write CSV
    outfile = "/Users/Jackson/.openclaw/workspace/research/Tractors/us_farm_equipment_trade.csv"
    with open(outfile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Year",
            "8701_All_Tractors_Import_USD",
            "8701_All_Tractors_Export_USD",
            "8701_All_Tractors_Trade_Balance_USD",
            "Ag_Tractor_Import_USD",
            "Ag_Tractor_Import_Units",
            "Ag_Tractor_Export_USD",
            "Ag_Tractor_Export_Units",
            "Ag_Tractor_Trade_Balance_USD",
            "8433_All_Harvest_Import_USD",
            "8433_All_Harvest_Export_USD",
            "8433_All_Harvest_Trade_Balance_USD",
            "Combine_Import_USD",
            "Combine_Import_Units",
            "Combine_Export_USD",
            "Combine_Export_Units",
            "Combine_Trade_Balance_USD",
        ])

        for year in YEARS:
            imp_8701 = imp_8701_hs4.get(year)
            exp_8701 = exp_8701_hs4.get(year)
            bal_8701 = (exp_8701 - imp_8701) if (imp_8701 and exp_8701) else None

            ag_imp_val = ag_tractor_imp.get(year, {}).get("value")
            ag_imp_units = ag_tractor_imp.get(year, {}).get("units")
            ag_exp_val = ag_tractor_exp.get(year, {}).get("value")
            ag_exp_units = ag_tractor_exp.get(year, {}).get("units")
            ag_bal = (ag_exp_val - ag_imp_val) if (ag_imp_val and ag_exp_val) else None

            imp_8433 = imp_8433_hs4.get(year)
            exp_8433 = exp_8433_hs4.get(year)
            bal_8433 = (exp_8433 - imp_8433) if (imp_8433 and exp_8433) else None

            comb_imp_val = combine_imp.get(year, {}).get("value")
            comb_imp_units = combine_imp.get(year, {}).get("units")
            comb_exp_val = combine_exp.get(year, {}).get("value")
            comb_exp_units = combine_exp.get(year, {}).get("units")
            comb_bal = (comb_exp_val - comb_imp_val) if (comb_imp_val and comb_exp_val) else None

            writer.writerow([
                year,
                imp_8701, exp_8701, bal_8701,
                ag_imp_val, ag_imp_units,
                ag_exp_val, ag_exp_units,
                ag_bal,
                imp_8433, exp_8433, bal_8433,
                comb_imp_val, comb_imp_units,
                comb_exp_val, comb_exp_units,
                comb_bal,
            ])

    print(f"\nData written to {outfile}", file=sys.stderr)

    # Also print a nice summary table
    print("\n" + "=" * 120)
    print(f"{'Year':>6} | {'AG TRACTOR IMPORTS':>30} | {'AG TRACTOR EXPORTS':>30} | {'COMBINE IMPORTS':>25} | {'COMBINE EXPORTS':>25}")
    print(f"{'':>6} | {'$M':>12} {'Units':>8} {'$/Unit':>9} | {'$M':>12} {'Units':>8} {'$/Unit':>9} | {'$M':>12} {'Units':>6} {'$/Unit':>6} | {'$M':>12} {'Units':>6} {'$/Unit':>6}")
    print("-" * 120)

    for year in YEARS:
        ag_iv = ag_tractor_imp.get(year, {}).get("value")
        ag_iu = ag_tractor_imp.get(year, {}).get("units")
        ag_ev = ag_tractor_exp.get(year, {}).get("value")
        ag_eu = ag_tractor_exp.get(year, {}).get("units")
        ci_v = combine_imp.get(year, {}).get("value")
        ci_u = combine_imp.get(year, {}).get("units")
        ce_v = combine_exp.get(year, {}).get("value")
        ce_u = combine_exp.get(year, {}).get("units")

        def fmt_m(v): return f"${v/1e6:>10,.1f}" if v else f"{'N/A':>11}"
        def fmt_u(u): return f"{u:>8,}" if u else f"{'N/A':>8}"
        def fmt_pu(v, u): return f"${v/u:>7,.0f}" if (v and u) else f"{'N/A':>8}"

        print(f"{year:>6} | {fmt_m(ag_iv)} {fmt_u(ag_iu)} {fmt_pu(ag_iv, ag_iu)} | {fmt_m(ag_ev)} {fmt_u(ag_eu)} {fmt_pu(ag_ev, ag_eu)} | {fmt_m(ci_v)} {fmt_u(ci_u)} {fmt_pu(ci_v, ci_u)} | {fmt_m(ce_v)} {fmt_u(ce_u)} {fmt_pu(ce_v, ce_u)}")

    print("\n" + "=" * 120)
    print("HS4-LEVEL TOTALS (includes all subcategories)")
    print(f"{'Year':>6} | {'8701 Import $M':>15} | {'8701 Export $M':>15} | {'8701 Balance $M':>16} | {'8433 Import $M':>15} | {'8433 Export $M':>15} | {'8433 Balance $M':>16}")
    print("-" * 110)
    for year in YEARS:
        iv = imp_8701_hs4.get(year)
        ev = exp_8701_hs4.get(year)
        bv = (ev - iv) if (iv and ev) else None
        i4 = imp_8433_hs4.get(year)
        e4 = exp_8433_hs4.get(year)
        b4 = (e4 - i4) if (i4 and e4) else None
        def fm(v): return f"${v/1e6:>12,.1f}" if v else f"{'N/A':>13}"
        print(f"{year:>6} | {fm(iv)} | {fm(ev)} | {fm(bv)} | {fm(i4)} | {fm(e4)} | {fm(b4)}")


if __name__ == "__main__":
    main()
