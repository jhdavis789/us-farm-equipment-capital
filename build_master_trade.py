#!/usr/bin/env python3
"""
Combine UN Comtrade (2000-2009) and Census Bureau (2010-2024) data
into a single master trade data CSV for farm equipment.
"""
import csv
import sys

# UN Comtrade data for 2000-2009 (HS4 level - ALL tractors including road tractors)
# Format: year, 8701_imp_usd, 8701_imp_units, 8701_exp_usd, 8701_exp_units, 8433_imp_usd, 8433_exp_usd
comtrade_data = {
    2000: {"8701_imp": 3475619823, "8701_imp_u": 163918, "8701_exp": 2606637039, "8701_exp_u": 69142, "8433_imp": 578959994, "8433_exp": 1546445106},
    2001: {"8701_imp": 2520490073, "8701_imp_u": 139883, "8701_exp": 1976346379, "8701_exp_u": 56007, "8433_imp": 627613522, "8433_exp": 1396694736},
    2002: {"8701_imp": 3237171962, "8701_imp_u": 185365, "8701_exp": 2385044906, "8701_exp_u": 61760, "8433_imp": 671732979, "8433_exp": 1508916501},
    2003: {"8701_imp": 3264208251, "8701_imp_u": 213397, "8701_exp": 2712581478, "8701_exp_u": 63717, "8433_imp": 864993164, "8433_exp": 1707315183},
    2004: {"8701_imp": 4914007033, "8701_imp_u": 252360, "8701_exp": 3397441790, "8701_exp_u": 73593, "8433_imp": 1021103242, "8433_exp": 1921937985},
    2005: {"8701_imp": 5816640547, "8701_imp_u": 255310, "8701_exp": 4058808564, "8701_exp_u": 92095, "8433_imp": 1132984986, "8433_exp": 2395235359},
    2006: {"8701_imp": 6344055416, "8701_imp_u": 243969, "8701_exp": 4653694992, "8701_exp_u": 95417, "8433_imp": 1145651786, "8433_exp": 2348998550},
    2007: {"8701_imp": 4046160180, "8701_imp_u": 192132, "8701_exp": 4860596631, "8701_exp_u": 102914, "8433_imp": 1363941539, "8433_exp": 2697577510},
    2008: {"8701_imp": 5260250772, "8701_imp_u": 191624, "8701_exp": 5889456231, "8701_exp_u": 139805, "8433_imp": 1667668382, "8433_exp": 3337423140},
    2009: {"8701_imp": 4388308289, "8701_imp_u": 132787, "8701_exp": 3711067144, "8701_exp_u": 79459, "8433_imp": 1395260038, "8433_exp": 2843286269},
}

# Census Bureau HS10-level agricultural tractor and combine data (2010-2024)
# From us_farm_equipment_trade.csv (already pulled)
census_detail = {}
with open("/Users/Jackson/.openclaw/workspace/research/Tractors/us_farm_equipment_trade.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        year = int(row["Year"])
        census_detail[year] = row

# Census Bureau HS4 level + Comtrade quantities for 2010-2024
census_hs4 = {}
with open("/Users/Jackson/.openclaw/workspace/research/Tractors/us_farm_equipment_trade_full.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        year = int(row["Year"])
        if year >= 2010:
            census_hs4[year] = row

# Write master file
outfile = "/Users/Jackson/.openclaw/workspace/research/Tractors/us_farm_equipment_trade_master.csv"
with open(outfile, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "Year",
        "Source",
        # HS 8701 - ALL Tractors (HS4 level)
        "HS8701_All_Tractor_Import_USD",
        "HS8701_All_Tractor_Import_Units",
        "HS8701_All_Tractor_Export_USD",
        "HS8701_All_Tractor_Export_Units",
        "HS8701_All_Tractor_Trade_Balance_USD",
        # Agricultural Tractors (HS10 detail, 2010+ only)
        "Ag_Tractor_Import_USD",
        "Ag_Tractor_Import_Units",
        "Ag_Tractor_Export_USD",
        "Ag_Tractor_Export_Units",
        "Ag_Tractor_Trade_Balance_USD",
        "Ag_Tractor_Avg_Import_Price_USD",
        "Ag_Tractor_Avg_Export_Price_USD",
        # HS 8433 - ALL Harvesting/Threshing (HS4 level)
        "HS8433_All_Harvest_Import_USD",
        "HS8433_All_Harvest_Export_USD",
        "HS8433_All_Harvest_Trade_Balance_USD",
        # Combine Harvester-Threshers (HS10 detail, 2010+ only)
        "Combine_Import_USD",
        "Combine_Import_Units",
        "Combine_Export_USD",
        "Combine_Export_Units",
        "Combine_Trade_Balance_USD",
        "Combine_Avg_Import_Price_USD",
        "Combine_Avg_Export_Price_USD",
    ])

    for year in range(2000, 2025):
        if year < 2010:
            # Comtrade only
            d = comtrade_data[year]
            bal_8701 = d["8701_exp"] - d["8701_imp"]
            bal_8433 = d["8433_exp"] - d["8433_imp"]
            writer.writerow([
                year, "UN Comtrade",
                d["8701_imp"], d["8701_imp_u"], d["8701_exp"], d["8701_exp_u"], bal_8701,
                "", "", "", "", "", "", "",
                d["8433_imp"], d["8433_exp"], bal_8433,
                "", "", "", "", "", "", "",
            ])
        else:
            # Census Bureau for values, Comtrade for HS4 unit counts
            cd = census_detail.get(year, {})
            ch = census_hs4.get(year, {})

            imp_8701 = int(cd.get("8701_All_Import_USD", 0) or 0)
            exp_8701 = int(cd.get("8701_All_Export_USD", 0) or 0)
            bal_8701 = exp_8701 - imp_8701

            imp_8701_u = int(ch.get("8701_All_Import_Units", 0) or 0) if ch else 0
            exp_8701_u = int(ch.get("8701_All_Export_Units", 0) or 0) if ch else 0

            ag_imp = int(cd.get("Ag_Tractor_Import_USD", 0) or 0)
            ag_imp_u = int(cd.get("Ag_Tractor_Import_Units", 0) or 0)
            ag_exp = int(cd.get("Ag_Tractor_Export_USD", 0) or 0)
            ag_exp_u = int(cd.get("Ag_Tractor_Export_Units", 0) or 0)
            ag_bal = ag_exp - ag_imp if (ag_imp and ag_exp) else ""
            ag_avg_imp = round(ag_imp / ag_imp_u) if ag_imp_u else ""
            ag_avg_exp = round(ag_exp / ag_exp_u) if ag_exp_u else ""

            imp_8433 = int(cd.get("8433_All_Import_USD", 0) or 0)
            exp_8433 = int(cd.get("8433_All_Export_USD", 0) or 0)
            bal_8433 = exp_8433 - imp_8433

            comb_imp = int(cd.get("Combine_Import_USD", 0) or 0)
            comb_imp_u = int(cd.get("Combine_Import_Units", 0) or 0)
            comb_exp = int(cd.get("Combine_Export_USD", 0) or 0)
            comb_exp_u = int(cd.get("Combine_Export_Units", 0) or 0)
            comb_bal = comb_exp - comb_imp if (comb_imp and comb_exp) else ""
            comb_avg_imp = round(comb_imp / comb_imp_u) if comb_imp_u else ""
            comb_avg_exp = round(comb_exp / comb_exp_u) if comb_exp_u else ""

            writer.writerow([
                year, "Census Bureau",
                imp_8701, imp_8701_u or "", exp_8701, exp_8701_u or "", bal_8701,
                ag_imp or "", ag_imp_u or "", ag_exp or "", ag_exp_u or "", ag_bal, ag_avg_imp, ag_avg_exp,
                imp_8433, exp_8433, bal_8433,
                comb_imp or "", comb_imp_u or "", comb_exp or "", comb_exp_u or "", comb_bal, comb_avg_imp, comb_avg_exp,
            ])

print(f"Master file written to {outfile}")

# Print summary table
print()
print("=" * 180)
print(f"{'Year':>6} | {'ALL 8701 TRACTORS':^50} | {'AG TRACTORS (HS10)':^50} | {'8433 HARVEST':^30} | {'COMBINES (HS10)':^40}")
print(f"{'':>6} | {'Import $M':>12} {'Export $M':>12} {'Balance $M':>12} {'Imp Units':>12} | {'Import $M':>10} {'Units':>8} {'$/Unit':>8} {'Export $M':>10} {'Units':>6} | {'Import $M':>12} {'Export $M':>12} | {'Imp $M':>8} {'Units':>6} {'Exp $M':>8} {'Units':>6}")
print("-" * 180)

for year in range(2000, 2025):
    if year < 2010:
        d = comtrade_data[year]
        bal = (d["8701_exp"] - d["8701_imp"]) / 1e6
        print(f"{year:>6} | {d['8701_imp']/1e6:>11,.0f}  {d['8701_exp']/1e6:>11,.0f}  {bal:>11,.0f}  {d['8701_imp_u']:>11,} | {'N/A':>10} {'N/A':>8} {'N/A':>8} {'N/A':>10} {'N/A':>6} | {d['8433_imp']/1e6:>11,.0f}  {d['8433_exp']/1e6:>11,.0f} | {'N/A':>8} {'N/A':>6} {'N/A':>8} {'N/A':>6}")
    else:
        cd = census_detail.get(year, {})
        imp = int(cd.get("8701_All_Import_USD", 0))
        exp = int(cd.get("8701_All_Export_USD", 0))
        bal = (exp - imp) / 1e6
        ag_iv = int(cd.get("Ag_Tractor_Import_USD", 0))
        ag_iu = int(cd.get("Ag_Tractor_Import_Units", 0))
        ag_ev = int(cd.get("Ag_Tractor_Export_USD", 0))
        ag_eu = int(cd.get("Ag_Tractor_Export_Units", 0))
        ag_pp = ag_iv // ag_iu if ag_iu else 0
        i4 = int(cd.get("8433_All_Import_USD", 0))
        e4 = int(cd.get("8433_All_Export_USD", 0))
        ci = int(cd.get("Combine_Import_USD", 0))
        ciu = int(cd.get("Combine_Import_Units", 0))
        ce = int(cd.get("Combine_Export_USD", 0))
        ceu = int(cd.get("Combine_Export_Units", 0))
        print(f"{year:>6} | {imp/1e6:>11,.0f}  {exp/1e6:>11,.0f}  {bal:>11,.0f}  {'':>11} | {ag_iv/1e6:>9,.0f}  {ag_iu:>7,}  ${ag_pp:>6,}  {ag_ev/1e6:>9,.0f}  {ag_eu:>5,} | {i4/1e6:>11,.0f}  {e4/1e6:>11,.0f} | {ci/1e6:>7,.0f}  {ciu:>5,}  {ce/1e6:>7,.0f}  {ceu:>5,}")
