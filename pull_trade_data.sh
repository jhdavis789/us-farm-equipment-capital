#!/bin/bash
# Pull US Farm Equipment Trade Data from Census Bureau API
# Uses curl to avoid Python SSL issues
# Outputs CSV to stdout

IMPORT_BASE="https://api.census.gov/data/timeseries/intltrade/imports/hs"
EXPORT_BASE="https://api.census.gov/data/timeseries/intltrade/exports/hs"

OUTFILE="/Users/Jackson/.openclaw/workspace/research/Tractors/us_farm_equipment_trade.csv"

# Helper: extract value from Census API JSON response
# Usage: extract_val "JSON_RESPONSE" FIELD_INDEX
extract_val() {
    echo "$1" | python3 -c "
import sys, json, ssl
ssl._create_default_https_context = ssl._create_unverified_context
try:
    data = json.load(sys.stdin)
    if len(data) > 1:
        print(data[1][$2])
    else:
        print('0')
except:
    print('0')
"
}

# Helper: sum values from multi-row Census API JSON response for specific columns
# Usage: sum_vals "JSON_RESPONSE" VAL_INDEX QTY_INDEX
sum_vals() {
    echo "$1" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    total_val = 0
    total_qty = 0
    for row in data[1:]:
        v = row[$2]
        q = row[$3]
        if v and v != '0' and v != '-':
            total_val += int(v)
        if q and q != '0' and q != '-':
            total_qty += int(q)
    print(f'{total_val},{total_qty}')
except:
    print('0,0')
"
}

echo "Year,8701_All_Import_USD,8701_All_Export_USD,Ag_Tractor_Import_USD,Ag_Tractor_Import_Units,Ag_Tractor_Export_USD,Ag_Tractor_Export_Units,8433_All_Import_USD,8433_All_Export_USD,Combine_Import_USD,Combine_Import_Units,Combine_Export_USD,Combine_Export_Units" > "$OUTFILE"

for year in $(seq 2010 2024); do
    echo "Processing $year..." >&2

    # 1. HS4 8701 Import
    resp=$(curl -s "${IMPORT_BASE}?get=I_COMMODITY,CON_VAL_YR&COMM_LVL=HS4&I_COMMODITY=8701&YEAR=${year}&MONTH=12")
    imp_8701=$(extract_val "$resp" 1)
    sleep 0.2

    # 2. HS4 8701 Export
    resp=$(curl -s "${EXPORT_BASE}?get=E_COMMODITY,ALL_VAL_YR&COMM_LVL=HS4&E_COMMODITY=8701&YEAR=${year}&MONTH=12")
    exp_8701=$(extract_val "$resp" 1)
    sleep 0.2

    # 3. HS4 8433 Import
    resp=$(curl -s "${IMPORT_BASE}?get=I_COMMODITY,CON_VAL_YR&COMM_LVL=HS4&I_COMMODITY=8433&YEAR=${year}&MONTH=12")
    imp_8433=$(extract_val "$resp" 1)
    sleep 0.2

    # 4. HS4 8433 Export
    resp=$(curl -s "${EXPORT_BASE}?get=E_COMMODITY,ALL_VAL_YR&COMM_LVL=HS4&E_COMMODITY=8433&YEAR=${year}&MONTH=12")
    exp_8433=$(extract_val "$resp" 1)
    sleep 0.2

    # 5. Ag Tractor Imports (HS10)
    if [ $year -lt 2017 ]; then
        resp=$(curl -s "${IMPORT_BASE}?get=I_COMMODITY,I_COMMODITY_LDESC,CON_VAL_YR,CON_QY1_YR,UNIT_QY1&COMM_LVL=HS10&I_COMMODITY=87019010*&YEAR=${year}&MONTH=12")
        ag_imp=$(sum_vals "$resp" 2 3)
    else
        # Sum all ag tractor codes
        total_val=0
        total_qty=0
        for code in 87019110 87019210 87019310 87019410 87019510; do
            resp=$(curl -s "${IMPORT_BASE}?get=I_COMMODITY,I_COMMODITY_LDESC,CON_VAL_YR,CON_QY1_YR,UNIT_QY1&COMM_LVL=HS10&I_COMMODITY=${code}*&YEAR=${year}&MONTH=12")
            vq=$(sum_vals "$resp" 2 3)
            v=$(echo "$vq" | cut -d, -f1)
            q=$(echo "$vq" | cut -d, -f2)
            total_val=$((total_val + v))
            total_qty=$((total_qty + q))
            sleep 0.2
        done
        ag_imp="${total_val},${total_qty}"
    fi
    ag_imp_val=$(echo "$ag_imp" | cut -d, -f1)
    ag_imp_units=$(echo "$ag_imp" | cut -d, -f2)
    sleep 0.2

    # 6. Ag Tractor Exports (HS10)
    if [ $year -lt 2017 ]; then
        resp=$(curl -s "${EXPORT_BASE}?get=E_COMMODITY,E_COMMODITY_LDESC,ALL_VAL_YR,QTY_1_YR,UNIT_QY1&COMM_LVL=HS10&E_COMMODITY=87019010*&YEAR=${year}&MONTH=12")
        ag_exp=$(sum_vals "$resp" 2 3)
    else
        total_val=0
        total_qty=0
        for code in 87019110 87019210 87019310 87019410 87019510; do
            resp=$(curl -s "${EXPORT_BASE}?get=E_COMMODITY,E_COMMODITY_LDESC,ALL_VAL_YR,QTY_1_YR,UNIT_QY1&COMM_LVL=HS10&E_COMMODITY=${code}*&YEAR=${year}&MONTH=12")
            vq=$(sum_vals "$resp" 2 3)
            v=$(echo "$vq" | cut -d, -f1)
            q=$(echo "$vq" | cut -d, -f2)
            total_val=$((total_val + v))
            total_qty=$((total_qty + q))
            sleep 0.2
        done
        ag_exp="${total_val},${total_qty}"
    fi
    ag_exp_val=$(echo "$ag_exp" | cut -d, -f1)
    ag_exp_units=$(echo "$ag_exp" | cut -d, -f2)
    sleep 0.2

    # 7. Combine Imports (HS10 843351*)
    resp=$(curl -s "${IMPORT_BASE}?get=I_COMMODITY,I_COMMODITY_LDESC,CON_VAL_YR,CON_QY1_YR,UNIT_QY1&COMM_LVL=HS10&I_COMMODITY=843351*&YEAR=${year}&MONTH=12")
    comb_imp=$(sum_vals "$resp" 2 3)
    comb_imp_val=$(echo "$comb_imp" | cut -d, -f1)
    comb_imp_units=$(echo "$comb_imp" | cut -d, -f2)
    sleep 0.2

    # 8. Combine Exports (HS10 843351*)
    resp=$(curl -s "${EXPORT_BASE}?get=E_COMMODITY,E_COMMODITY_LDESC,ALL_VAL_YR,QTY_1_YR,UNIT_QY1&COMM_LVL=HS10&E_COMMODITY=843351*&YEAR=${year}&MONTH=12")
    comb_exp=$(sum_vals "$resp" 2 3)
    comb_exp_val=$(echo "$comb_exp" | cut -d, -f1)
    comb_exp_units=$(echo "$comb_exp" | cut -d, -f2)
    sleep 0.2

    echo "${year},${imp_8701},${exp_8701},${ag_imp_val},${ag_imp_units},${ag_exp_val},${ag_exp_units},${imp_8433},${exp_8433},${comb_imp_val},${comb_imp_units},${comb_exp_val},${comb_exp_units}" >> "$OUTFILE"
    echo "  Done: 8701 imp=$imp_8701 exp=$exp_8701 | ag_imp=$ag_imp_val/$ag_imp_units ag_exp=$ag_exp_val/$ag_exp_units | comb_imp=$comb_imp_val/$comb_imp_units comb_exp=$comb_exp_val/$comb_exp_units" >&2
done

echo "" >&2
echo "Output written to $OUTFILE" >&2
echo "" >&2
cat "$OUTFILE"
