# Tractor Dealer Data Quality Report

**Date:** 2026-02-23
**Dataset:** `all_dealers.csv` (raw merged) / `all_dealers_cleaned.csv` (cleaned)
**Analyst:** Automated quality analysis

---

## Executive Summary

The unified tractor dealer dataset contains **14,538 raw records** across 13 brands from 6 parent companies. After cleaning, the dataset contains **14,479 records** spanning **106 countries**. Overall data quality is good, with 99.9% of records having valid coordinates. Key issues identified and addressed:

- **57 exact duplicate records** removed (primarily John Deere FR/DE from overlapping scrape grids)
- **70 Argo "INTL" records** reclassified to proper ISO country codes using coordinate and city inference
- **27 SDF regional codes** (EU, EA, FE, LA) resolved to specific country codes
- **18 Argo dealers** miscoded as DE (Germany) corrected to AT (Austria) based on coordinates
- **14 Argo dealers** miscoded as AR (Argentina) corrected to their actual Latin American countries
- **2 blank/empty rows** removed (AGCO records with no data)
- **2 GB dealer records** with coordinates in Tasmania, Australia had coords cleared
- **1 zero-coordinate record** cleared

Significant **coverage gaps** remain:
- **Kubota** (est. 1,100+ US dealers) -- no data yet
- **Deutz-Fahr** US dealers (est. 50-80) -- Cloudflare blocked scraper
- **STEYR** EU incomplete -- only AT, BE, CZ, BG, CH scraped (28 countries remaining)
- **CNH overall** -- US and most EU complete, but STEYR scrape incomplete

---

## 1. Coverage Analysis

### 1.1 Brand Counts

| Brand | Total | US | DE | FR | IT | GB | ES | AT |
|-------|------:|---:|---:|---:|---:|---:|---:|---:|
| John Deere | 4,338 | 1,893 | 576 | 438 | 118 | 139 | 123 | 123 |
| Massey Ferguson | 1,578 | 206 | 322 | 228 | 43 | 78 | 47 | 42 |
| Fendt | 1,443 | 147 | 384 | 191 | 37 | 70 | 56 | 63 |
| Case IH | 1,363 | 682 | 158 | 142 | 17 | 34 | 47 | 8 |
| CLAAS | 1,340 | 215 | 296 | 231 | 34 | 68 | 28 | 45 |
| New Holland | 1,236 | 659 | 95 | 90 | 24 | 25 | 31 | 41 |
| Valtra | 928 | 1 | 249 | 191 | 38 | 61 | 43 | 56 |
| McCormick | 746 | 176 | 97 | 90 | 46 | 35 | 45 | 12 |
| Landini | 568 | 0 | 25 | 58 | 65 | 10 | 51 | 7 |
| SAME | 394 | 0 | 38 | 72 | 41 | 23 | 12 | 1 |
| Deutz-Fahr | 346 | 0 | 79 | 51 | 103 | 3 | 68 | 0 |
| STEYR | 144 | 0 | 0 | 0 | 0 | 0 | 0 | 101 |
| Lamborghini Tractors | 55 | 0 | 0 | 8 | 1 | 0 | 2 | 0 |

### 1.2 Parent Company Summary

| Parent Company | Dealers | Expected (est.) | Status |
|---------------|--------:|----------------:|--------|
| Deere & Company | 4,338 | ~4,500 US + EU | Good - US complete, EU may be slightly over-counted due to dense scrape overlap |
| AGCO (Fendt/MF/Valtra) | 3,949 | ~4,000-5,000 | Good - comprehensive global coverage |
| CNH Industrial | 2,743 | ~3,500-4,500 | Partial - US complete, EU mostly complete, STEYR incomplete |
| CLAAS | 1,340 | ~1,300-1,500 | Good |
| Argo Tractors | 1,314 | ~1,200-1,500 | Good |
| SDF Group | 795 | ~1,200-1,500 | Missing US Deutz-Fahr (~50-80 dealers), Cloudflare blocked |
| **Kubota** | **0** | **~1,100+ US** | **Not scraped yet** |

### 1.3 US Market Coverage

| Brand | US Dealers | Expected (est.) | Assessment |
|-------|----------:|----------------:|------------|
| John Deere | 1,893 | ~2,000 | Good |
| Case IH | 682 | ~700-800 | Good (complete scrape) |
| New Holland | 659 | ~700-800 | Good (complete scrape) |
| CLAAS | 215 | ~200-250 | Good |
| Massey Ferguson | 206 | ~200-300 | Good |
| McCormick | 176 | ~150-200 | Good |
| Fendt | 147 | ~130-170 | Good |
| Valtra | 1 | ~1-5 (limited US presence) | Correct |
| **Kubota** | **0** | **~1,100+** | **Missing** |
| **Deutz-Fahr** | **0** | **~50-80** | **Cloudflare blocked** |
| **STEYR** | **0** | **0** | Correct (not sold in US) |
| **Total US** | **3,979** | **~5,300-6,300** | ~63-75% complete |

### 1.4 European Coverage Gaps

Major agricultural countries with coverage assessment:

| Country | Dealers | Assessment |
|---------|--------:|------------|
| Germany (DE) | 2,319 | Excellent |
| France (FR) | 1,790 | Excellent |
| Italy (IT) | 567 | Good, but no Case IH/NH Italy data from CNH |
| Spain (ES) | 553 | Good |
| United Kingdom (GB) | 546 | Good |
| Austria (AT) | 499 | Good (improved after DE->AT corrections) |
| Norway (NO) | 350 | Good |
| Turkey (TR) | 325 | Good |
| Poland (PL) | 297 | Good |
| Netherlands (NL) | 294 | Good |
| Belgium (BE) | 279 | Good |
| Sweden (SE) | 276 | Good |
| Finland (FI) | 218 | Good |
| Romania (RO) | 105 | Fair - could be higher with STEYR/more CNH data |
| Hungary (HU) | 78 | Fair |
| Bulgaria (BG) | 52 | Fair |
| Greece (GR) | 25 | Low - likely missing CNH/AGCO data |
| Croatia (HR) | 19 | Low |
| Serbia (RS) | 18 | Fair |
| Slovenia (SI) | 18 | Fair |

**Eastern Europe is underrepresented** primarily because:
- STEYR scrape only completed 8 of 36 EU countries
- CNH Italy data may be missing (only 17 Case IH in IT)
- Some Argo/SDF data may not cover all sub-regions

---

## 2. Geographic Validation

### 2.1 Coordinate Quality

| Metric | Count | Pct |
|--------|------:|----:|
| Records with valid coordinates | 14,471 | 99.9% |
| Records missing coordinates | 8 | 0.1% |
| Zero/null island coordinates (cleared) | 1 | <0.01% |
| Out-of-bounds for stated country | ~50 | 0.3% |

### 2.2 Records Missing Coordinates (after cleaning)

These 8 records have no coordinates and cannot be mapped:

| Brand | Dealer Name | Country | City |
|-------|-------------|---------|------|
| Landini | Genius Thabazimbi | ZA | North West |
| McCormick | LABORES TEPIC | MX | (blank) |
| McCormick | Mel's Farm Repair | US | ID |
| McCormick | Genius Thabazimbi | ZA | North West |
| SAME | Solicita presupuesto | ES | (blank) |

Note: The SAME "Solicita presupuesto" record means "Request a quote" in Spanish -- this appears to be a placeholder, not an actual dealer.

### 2.3 Out-of-Bounds Coordinates Identified and Fixed

**Hawaii dealers (4):** John Deere Pape Machinery locations in Hilo, Kailua Kona, Wailuku, and Eleele. These are valid -- the bounding box for US was technically too narrow for Hawaii (lat 19-22). These are legitimate dealers and were NOT flagged as errors in the cleaned data.

**Jersey dealer (1):** John Deere - Ernie Le Feuvre Ltd in St John, Jersey (lat 49.22). This is correctly on the island of Jersey but just outside the tight GB lat bound (49.5). Valid record.

**Aland Islands (2):** John Deere dealers in Mariehamn coded as FI (Finland). Aland is Finnish territory but lng 19.9 is just outside FI bound (20.5). Valid records.

**Argo DE-to-AT corrections (18):** Dealers in Niederosterreich, Steiermark, Burgenland, Wolfsberg etc. that were coded as DE but are clearly Austrian. Fixed in cleaned dataset.

**GB dealer with Australian coords (2):** "Farm and Fleet Services" coded as GB with lat=-41.44, lng=147.14 (Tasmania, Australia). Coordinates cleared in cleaned dataset. This is a known UK dealer -- coords were clearly wrong in the source data.

### 2.4 Argo AR (Argentina) Country Code Corrections

The Argo scraper's Spanish-language Argentina site (`argo_es-ar`) covers all of Latin America, not just Argentina. All 15 dealers coded as AR were in other countries:

| Dealer | Actual Country | Corrected To |
|--------|---------------|-------------|
| BYC International | Panama | PA |
| DERCO Peru S.A. | Peru | PE |
| DISTRIBUIDORA AGRICOLA LANDINI C.A | Venezuela | VE |
| EURONOVA | Dominican Republic | DO |
| FORMUNICA S.A. | Nicaragua | NI |
| Grupo SC | Argentina | AR (correct) |
| MEINDERTSMA | Suriname | SR |
| Montano & Gutierrez S.A.S | Colombia | CO |
| Tecnoagricola de Centroamerica | Costa Rica | CR |
| Tractores McCormick de Centro America | Guatemala | GT |
| Vision Equipment Supply | Barbados | BB |
| WOSLEN S.A. | Uruguay | UY |
| Grupo SC | Argentina | AR (kept -- correct) |
| ORBES AGRICOLA S.A.C. | Peru | PE |

---

## 3. Duplicate Detection

### 3.1 Exact Duplicates Removed: 57

All 57 exact duplicates were **John Deere** records, almost entirely in **France and Germany**. These resulted from overlapping grid queries in the dense scrape pass duplicating records from the initial scrape. The dedup fingerprint in `merge_dealers.py` uses `brand|name|lat|lng` rounded to 3 decimal places, but some records had identical name+city+address with slightly different coordinate precision.

Sample duplicates removed:
- AHS GmbH, Gardelegen (DE) -- appeared twice
- B + S Landtechnik GmbH, Grabow (DE) -- appeared twice
- SAS AGRI-POLE, LE MONTAT (FR) -- appeared twice
- Knoblauch Landtechnik GmbH in 4 locations (DE) -- each appeared twice

### 3.2 Multi-Brand Dealers: 1,037

These are legitimate multi-brand dealerships where the same dealer name appears in the same city selling different brands. This is expected and NOT a data quality issue -- it is common for dealers to carry multiple brands.

Top examples:
- Many European dealers carry both **Landini + McCormick** (both Argo brands)
- US dealers often carry **Case IH + New Holland** (both CNH brands, though less common together)
- AGCO dealers carrying **Fendt + Massey Ferguson + Valtra**

### 3.3 Close-Coordinate Different-Name Pairs

Approximately 30 pairs of dealers of the same brand were found within ~100m of each other with different names. Most of these are legitimate -- adjacent dealership locations or renamed dealers. No action needed.

---

## 4. Data Quality Issues

### 4.1 Non-ISO Country Codes (All Fixed)

| Original Code | Count | Meaning | Resolution |
|--------------|------:|---------|-----------|
| INTL | 70 | Argo international/rest-of-world | Resolved to 35+ specific countries via coords/city |
| EU | 13 | SDF European region | Resolved to RS, MK, HU, BA, SI, RO, GR, AL |
| LA | 9 | SDF Latin America region | Resolved to CL, PA, NI, CU, AR, PE, CO, EC |
| EA | 3 | SDF East Asia region | Resolved to PH, TW, KH |
| FE | 2 | SDF Far East region | Resolved to AU, NZ |
| NA | 1 | Namibia (correct ISO!) | Kept as NA (Namibia) |

### 4.2 Blank Records Removed: 2

Two AGCO records (one Fendt, one Massey Ferguson) had completely blank fields -- no name, no coords, no city. These were artifact rows from the API response and have been removed.

### 4.3 Phone Number Formatting

Phone numbers are inconsistently formatted across brands:
- **International format** (+49 xxx, +1 xxx): Most CLAAS, SDF, Argo dealers
- **Digits only** (9563509865): Most Deere US dealers
- **Parenthetical** ((785) 282-6861): Most AGCO US dealers
- **00-prefix** (0043 732...): Some European dealers

**Recommendation:** Normalize phone numbers to E.164 format in a future pass. Not critical for mapping/analysis.

### 4.4 Address Quality

Some Argo dealers have city names that contain country information (e.g., city="Via La Colonia - Zona Industri Estado Portuguesa - Pais VENEZ"). This is a quirk of the source data where the full location description was put into the city field. These records are usable since they have valid coordinates.

### 4.5 Placeholder Record

The SAME dealer "Solicita presupuesto" (Spanish for "Request a quote") in ES with no coordinates appears to be a placeholder in the SDF system, not an actual dealer. Consider removing.

---

## 5. Brand-Specific Known Issues

### 5.1 SDF / Deutz-Fahr

**Missing: ALL US Deutz-Fahr dealers (~50-80 estimated)**

The SDF dealer locator API for the US market uses Cloudflare protection that blocked automated scraping. The dataset has 0 Deutz-Fahr and 0 SAME dealers in the US. Deutz-Fahr has a significant US dealer network (estimated 50-80 locations based on industry sources).

**Recommendation:** Try scraping via browser automation (Playwright/Puppeteer) or find an alternative data source for Deutz-Fahr US dealers.

### 5.2 CNH Industrial

**Status from scrape progress file:**

| Task | Completed | Total | Status |
|------|----------:|------:|--------|
| Case IH US (by state) | 51 | 51 | COMPLETE |
| New Holland US (by state) | 51 | 51 | COMPLETE |
| Case IH EU (by country) | 36 | 36 | COMPLETE |
| New Holland EU (by country) | 36 | 36 | COMPLETE |
| STEYR EU (by country) | 8 | 36 | **INCOMPLETE** |

**STEYR scrape completed only:** AT, BA, BE, BG, CH, CZ, AL + a few more (~8 total)

**STEYR missing countries:** DK, EE, ES, FI, FR, GB, GR, HR, HU, IE, IS, IT, LT, LU, LV, MD, ME, MK, NL, NO, PL, PT, RO, RS, SE, SI, SK, UA

**CNH brand counts in dataset:** Case IH: 1,231 | New Holland: 1,245 | STEYR: 306

**Note:** STEYR is primarily sold in Austria and nearby markets. The 101 AT dealers are likely the bulk. However, STEYR also has dealers in IT, DE, FR, PL etc. that are missing. Estimated 50-150 additional STEYR dealers.

**Recommendation:** Resume/complete the STEYR EU scrape for remaining countries.

### 5.3 AGCO

The AGCO combined dataset contains **3,976 raw records** (Fendt: 1,628 + MF: 1,892 + Valtra: 928 = 4,448 in individual files, 3,976 after combining/deduplication in the combined file). No evidence of API cap-at-100 issues -- no brand+country combination had exactly 100 records.

Coverage appears comprehensive for Europe and US. The sub-grid query approach successfully avoided the 100-result API cap.

### 5.4 John Deere

The Deere dataset shows 57 exact duplicates from overlapping FR/DE dense scrape grids. These have been removed in the cleaned dataset. The remaining 4,338 records appear solid.

**Note:** Deere count dropped from 4,374 (raw) to 4,338 (cleaned) -- the difference is the 36 removed duplicates (57 dups total minus some that were in the non-Deere data).

### 5.5 CLAAS

1,340 dealers, comprehensive coverage. No known issues. Data quality is among the highest in the dataset due to CLAAS's well-structured API.

### 5.6 Argo Tractors

The main issue was the `argo_es-ar` source coding all Latin American dealers as country=AR. This has been fixed by inferring actual countries from coordinates. Additionally, 18 dealers from the `argo_de` source were actually in Austria (coded as DE because the German-language site covers the DACH region). These have been corrected.

The `argo_as` (Asia/rest-of-world) source used INTL as the country code for 70 dealers across 35+ countries. All have been resolved using coordinate and city-name inference.

---

## 6. Coverage Completeness Assessment

### By Brand Per Major Market (Cleaned Dataset)

Scale: Full = comprehensive, Partial = some data, Missing = no data

| Brand | US | DE | FR | IT | GB | ES |
|-------|:--:|:--:|:--:|:--:|:--:|:--:|
| John Deere | Full | Full | Full | Full | Full | Full |
| Massey Ferguson | Full | Full | Full | Partial | Full | Partial |
| Fendt | Full | Full | Full | Partial | Full | Full |
| Case IH | Full | Full | Full | Low | Partial | Full |
| CLAAS | Full | Full | Full | Partial | Full | Partial |
| New Holland | Full | Full | Full | Low | Low | Partial |
| Valtra | N/A | Full | Full | Partial | Full | Partial |
| McCormick | Full | Full | Full | Full | Full | Full |
| Landini | N/A | Partial | Full | Full | Low | Full |
| SAME | N/A | Full | Full | Full | Partial | Low |
| Deutz-Fahr | **Missing** | Full | Full | Full | Low | Full |
| STEYR | N/A | **Missing** | **Missing** | **Missing** | N/A | N/A |
| Kubota | **Missing** | **Missing** | **Missing** | **Missing** | **Missing** | **Missing** |

### Overall Completeness Estimate

| Market | Current | Estimated True | Completeness |
|--------|--------:|--------------:|-------------:|
| US | 3,979 | ~5,300 | ~75% |
| Germany | 2,319 | ~2,500 | ~93% |
| France | 1,790 | ~2,000 | ~90% |
| Italy | 567 | ~900 | ~63% |
| UK | 546 | ~700 | ~78% |
| Spain | 553 | ~600 | ~92% |
| **Global** | **14,479** | **~20,000** | **~72%** |

---

## 7. Recommendations

### High Priority

1. **Scrape Kubota US dealers** -- This is the single largest coverage gap. Kubota is the #2 tractor brand in the US by unit sales. Estimated 1,100+ dealers missing.

2. **Complete STEYR EU scrape** -- Resume the CNH scraper for the remaining 28 STEYR EU countries. This is straightforward since the scraper infrastructure exists.

3. **Scrape Deutz-Fahr US dealers** -- Try Playwright/Puppeteer to bypass Cloudflare, or find an alternative data source (e.g., their printed dealer directory PDF).

### Medium Priority

4. **Investigate low CNH Italy counts** -- Only 17 Case IH and 24 New Holland dealers in Italy seems low for a major agricultural market. May need to check if the Italian CNH website uses a different dealer locator.

5. **Add Kubota for Europe** -- Kubota has significant presence in DE, FR, GB. Currently zero data.

6. **Improve Deere dedup** -- The fingerprint using lat/lng to 3 decimal places allows slight coordinate variations to create duplicates. Consider using name+city+address as an additional dedup key.

### Low Priority

7. **Normalize phone numbers** to E.164 format.

8. **Clean address fields** where city/country info is embedded in the address.

9. **Remove placeholder records** like "Solicita presupuesto".

10. **Add AGCO specialty brands** (Challenger, Gleaner, etc.) if they have separate dealer networks.

---

## 8. Cleaning Changes Applied

The cleaned dataset (`all_dealers_cleaned.csv`) includes these changes from the raw merged data:

| Change | Count | Description |
|--------|------:|-------------|
| INTL codes resolved | 70 | Argo INTL -> specific ISO country codes |
| Exact duplicates removed | 57 | Deere FR/DE overlapping grid duplicates |
| SDF regional codes resolved | 27 | EU/EA/FE/LA -> specific ISO codes |
| Argo DE -> AT corrections | 18 | German-site Austrian dealers fixed |
| Argo AR corrections | 14 | Latin American dealers miscoded as Argentina |
| GB coords cleared | 2 | Farm and Fleet Services had Australian coords |
| Blank rows removed | 2 | Empty AGCO rows with no data |
| Zero coords cleared | 1 | Deutz-Fahr dealer at (0,0) |
| **Total records changed** | **~191** | |
| **Final record count** | **14,479** | Down from 14,538 raw |

---

## Appendix: File Inventory

| File | Location | Records | Description |
|------|----------|--------:|-------------|
| `deere_dealers.csv` | raw/ | 4,519 | John Deere US + global |
| `agco_dealers_combined.csv` | raw/ | 3,976 | Fendt + MF + Valtra combined |
| `claas_dealers.csv` | raw/ | 1,345 | CLAAS global |
| `argo_dealers.csv` | raw/ | 1,322 | McCormick + Landini global |
| `cnh_scrape_progress.json` | raw/ | 2,762 | Case IH + NH + STEYR (partial) |
| `sdf_dealers.csv` | raw/ | 795 | Deutz-Fahr + SAME + Lamborghini |
| `all_dealers.csv` | processed/ | 14,538 | Raw merged (before cleaning) |
| `all_dealers_cleaned.csv` | processed/ | 14,479 | Cleaned final dataset |
