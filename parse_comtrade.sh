#!/bin/bash
# Parse UN Comtrade data for 2000-2009 and merge with Census data for 2010-2024

OUTFILE="/Users/Jackson/.openclaw/workspace/research/Tractors/us_farm_equipment_trade_full.csv"
IMPORT_BASE="https://comtradeapi.un.org/public/v1/preview/C/A/HS"

echo "Year,8701_All_Import_USD,8701_All_Import_Units,8701_All_Export_USD,8701_All_Export_Units,8701_All_Import_NetWgt_kg,8701_All_Export_NetWgt_kg,8433_All_Import_USD,8433_All_Export_USD,8433_All_Import_NetWgt_kg,8433_All_Export_NetWgt_kg" > "$OUTFILE"

# 2000-2009 from UN Comtrade
for year in $(seq 2000 2009); do
    echo "Pulling Comtrade ${year}..." >&2

    # 8701
    resp=$(curl -s "${IMPORT_BASE}?reporterCode=842&period=${year}&partnerCode=0&flowCode=M,X&cmdCode=8701&customsCode=C00&motCode=0")
    parsed=$(echo "$resp" | python3 -c "
import sys, json
data = json.load(sys.stdin)
imp_val = imp_qty = imp_wgt = exp_val = exp_qty = exp_wgt = 0
for row in data.get('data', []):
    flow = row.get('flowCode', '')
    val = row.get('primaryValue', 0) or 0
    qty = row.get('qty', 0) or 0
    wgt = row.get('netWgt', 0) or 0
    if flow == 'M':
        imp_val = int(val)
        imp_qty = int(qty)
        imp_wgt = int(wgt)
    elif flow == 'X':
        exp_val = int(val)
        exp_qty = int(qty)
        exp_wgt = int(wgt)
print(f'{imp_val},{imp_qty},{exp_val},{exp_qty},{imp_wgt},{exp_wgt}')
")
    sleep 1.5

    # 8433
    resp2=$(curl -s "${IMPORT_BASE}?reporterCode=842&period=${year}&partnerCode=0&flowCode=M,X&cmdCode=8433&customsCode=C00&motCode=0")
    parsed2=$(echo "$resp2" | python3 -c "
import sys, json
data = json.load(sys.stdin)
imp_val = exp_val = imp_wgt = exp_wgt = 0
for row in data.get('data', []):
    flow = row.get('flowCode', '')
    val = row.get('primaryValue', 0) or 0
    wgt = row.get('netWgt', 0) or 0
    if flow == 'M':
        imp_val = int(val)
        imp_wgt = int(wgt)
    elif flow == 'X':
        exp_val = int(val)
        exp_wgt = int(wgt)
print(f'{imp_val},{exp_val},{imp_wgt},{exp_wgt}')
")
    sleep 1.5

    echo "${year},${parsed},${parsed2}" >> "$OUTFILE"
    echo "  ${year}: 8701=${parsed} | 8433=${parsed2}" >&2
done

echo "" >&2
echo "Comtrade data written. Now appending Census data..." >&2

# 2010-2024 from Census Bureau (already pulled, but also get HS4 unit counts from Comtrade)
for year in $(seq 2010 2024); do
    echo "Pulling Census+Comtrade ${year}..." >&2

    # Census HS4 values (more accurate for US data)
    imp_8701=$(curl -s "https://api.census.gov/data/timeseries/intltrade/imports/hs?get=I_COMMODITY,CON_VAL_YR&COMM_LVL=HS4&I_COMMODITY=8701&YEAR=${year}&MONTH=12" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[1][1] if len(d)>1 else 0)")
    sleep 0.2
    exp_8701=$(curl -s "https://api.census.gov/data/timeseries/intltrade/exports/hs?get=E_COMMODITY,ALL_VAL_YR&COMM_LVL=HS4&E_COMMODITY=8701&YEAR=${year}&MONTH=12" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[1][1] if len(d)>1 else 0)")
    sleep 0.2
    imp_8433=$(curl -s "https://api.census.gov/data/timeseries/intltrade/imports/hs?get=I_COMMODITY,CON_VAL_YR&COMM_LVL=HS4&I_COMMODITY=8433&YEAR=${year}&MONTH=12" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[1][1] if len(d)>1 else 0)")
    sleep 0.2
    exp_8433=$(curl -s "https://api.census.gov/data/timeseries/intltrade/exports/hs?get=E_COMMODITY,ALL_VAL_YR&COMM_LVL=HS4&E_COMMODITY=8433&YEAR=${year}&MONTH=12" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[1][1] if len(d)>1 else 0)")
    sleep 0.2

    # Try Comtrade for unit quantities at HS4 level
    resp=$(curl -s "${IMPORT_BASE}?reporterCode=842&period=${year}&partnerCode=0&flowCode=M,X&cmdCode=8701&customsCode=C00&motCode=0" 2>/dev/null)
    ct_parsed=$(echo "$resp" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    imp_qty = imp_wgt = exp_qty = exp_wgt = 0
    for row in data.get('data', []):
        flow = row.get('flowCode', '')
        qty = row.get('qty', 0) or 0
        wgt = row.get('netWgt', 0) or 0
        if flow == 'M':
            imp_qty = int(qty)
            imp_wgt = int(wgt)
        elif flow == 'X':
            exp_qty = int(qty)
            exp_wgt = int(wgt)
    print(f'{imp_qty},{exp_qty},{imp_wgt},{exp_wgt}')
except:
    print('0,0,0,0')
" 2>/dev/null)
    sleep 1.5

    ct_imp_qty=$(echo "$ct_parsed" | cut -d, -f1)
    ct_exp_qty=$(echo "$ct_parsed" | cut -d, -f2)
    ct_imp_wgt=$(echo "$ct_parsed" | cut -d, -f3)
    ct_exp_wgt=$(echo "$ct_parsed" | cut -d, -f4)

    # 8433 weights from Comtrade
    resp2=$(curl -s "${IMPORT_BASE}?reporterCode=842&period=${year}&partnerCode=0&flowCode=M,X&cmdCode=8433&customsCode=C00&motCode=0" 2>/dev/null)
    ct_parsed2=$(echo "$resp2" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    imp_wgt = exp_wgt = 0
    for row in data.get('data', []):
        flow = row.get('flowCode', '')
        wgt = row.get('netWgt', 0) or 0
        if flow == 'M':
            imp_wgt = int(wgt)
        elif flow == 'X':
            exp_wgt = int(wgt)
    print(f'{imp_wgt},{exp_wgt}')
except:
    print('0,0')
" 2>/dev/null)
    sleep 1.5

    ct_8433_imp_wgt=$(echo "$ct_parsed2" | cut -d, -f1)
    ct_8433_exp_wgt=$(echo "$ct_parsed2" | cut -d, -f2)

    echo "${year},${imp_8701},${ct_imp_qty},${exp_8701},${ct_exp_qty},${ct_imp_wgt},${ct_exp_wgt},${imp_8433},${exp_8433},${ct_8433_imp_wgt},${ct_8433_exp_wgt}" >> "$OUTFILE"
    echo "  ${year}: done" >&2
done

echo "" >&2
echo "Full data written to $OUTFILE" >&2
cat "$OUTFILE"
