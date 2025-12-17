import os
import sys
from pathlib import Path
import random
import math
import warnings
import numpy as np
import pandas as pd

# --- Brightway setup ---
import brightway2 as bw
from bw2data import geomapping
from bw2io.backup import backup_project_directory

# --- Your helpers (unchanged usage) ---
from helper_functions import (
    flush_biosphere_issues,
    flush_technosphere_issues,
    log_issue_bio,
    log_issue_tech,
    clear_technosphere_and_biosphere_exchanges,
    convert_amount_exchange_to_supplier

)

# ----------------------------- USER INPUTS -----------------------------
PROJECT_NAME        = "bw2uslci_generator_final"     # your existing project with newest USLCI
INPUT_DATASET_CSV   = "extracted_handcrafted_dataset_mapped.csv"  # <- your UPDATED dataset
ALLOCATION_CSV      = "allocation_exchange_df.csv"   # same format you already use
BRIDGE_CSV          = "working_bridge.csv"           # USLCI↔biosphere bridge you already have
ALLOC_TYPE          = "PHYSICAL_ALLOCATION"          # or existing type in your CSV
USER_DB_PREFIX      = "my_dataset_db_"               # new DB name will be randomized with this prefix
# Existing USER INPUTS...
USLCI_DB_NAME = "uslci_database596"   # <-- name of the *existing* USLCI database in the same project
# ----------------------------------------------------------------------

# IO + warnings behavior same as your other file
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None

# ------------------------------ PROJECT --------------------------------
bw.projects.set_current(PROJECT_NAME)
print(bw.databases)
bw.bw2setup()
project_name = PROJECT_NAME

# -------------------------- LOAD & PREP DATA ---------------------------
# test_data is the same name your current code uses for the USLCI CSV; we re-use it
# here to minimize downstream diffs.
test_data = pd.read_csv(INPUT_DATASET_CSV, low_memory=False)

# Normalize same way as your current code
test_data["process_location"] = test_data["process_location"].fillna("US")
test_data["exchange_supplying_process_id"] = test_data["exchange_supplying_process_id"].fillna("")
test_data["exchange_flow_type"] = test_data["exchange_type"]        # mirror your normalization
test_data["exchange_flow_name"] = test_data["exchange_name"]
test_data["exchange_flow_id"]   = test_data["exchange_id"]

# standard location cleanups (kept as-is in your script)
test_data["process_location"] = test_data["process_location"].replace(
    "Error:This process is missing location", "US"
)
test_data["process_location"] = test_data["process_location"].replace(
    "United States of America (the)", "US"
)
test_data["process_location"] = test_data["process_location"].replace(
    "Congo (the Democratic Republic of the)", "Congo"
)

# Drop waste flows just like your code
test_data = test_data[test_data["exchange_flow_type"] != "WASTE_FLOW"]

# product-level rows (same pattern)
process_product = test_data[
    [
        "process_name", "process_id", "process_location", "process_description",
        "exchange_flow_name", "exchange_flow_id", "exchange_amount", "exchange_unit",
        "exchange_ecoinvent_type"
    ]
]
process_product = process_product[process_product["exchange_ecoinvent_type"] == "production"]  # :contentReference[oaicite:0]{index=0}

# ---------------------- ALLOCATION (same structure) --------------------
allocation_df = pd.read_csv(ALLOCATION_CSV, low_memory=False)
alloc_type = ALLOC_TYPE

if alloc_type in set(allocation_df["allocation_type"].unique()):
    alloc_df_use = allocation_df[allocation_df["allocation_type"] == alloc_type].copy()
else:
    alloc_df_use = allocation_df.copy()
    print(f"Warning: '{alloc_type}' not found; using provided types:",
          sorted(alloc_df_use['allocation_type'].unique()))

# Align column names like your current script
alloc_df_use = alloc_df_use.rename(columns={
    "exchange_val": "allocation_factor",
    "exchange_id":  "exchange_flow_id",
})[["process_name", "process_id", "exchange_flow_id", "allocation_factor", "allocation_type"]]  # :contentReference[oaicite:1]{index=1}

# Attach "original keys" for merge (copy of your pattern)
corrected_df = process_product.copy()
corrected_df["_old_process_id"]      = corrected_df["process_id"]
corrected_df["_old_exchange_flow_id"] = corrected_df["exchange_flow_id"]

# Merge allocation onto corrected_df (kept same shape/semantics) :contentReference[oaicite:2]{index=2}
adf = alloc_df_use.rename(columns={
    "process_id":       "orig_process_id",
    "exchange_flow_id": "orig_exchange_id",
})
prod_df = corrected_df.rename(columns={
    "_old_process_id":       "orig_process_id",
    "_old_exchange_flow_id": "orig_exchange_id",
})

# inner merge: we only scale rows that have an allocation factor
merged = prod_df.merge(
    adf,
    how="left",
    on=["orig_process_id", "orig_exchange_id"],
    validate="m:1"
)

# If allocation_factor absent → default to 1.0 (same safeguard) :contentReference[oaicite:3]{index=3}
merged["allocation_factor"] = merged["allocation_factor"].astype(float)
merged["allocation_factor"] = merged["allocation_factor"].fillna(1.0)
bad_alloc = (merged["allocation_factor"] <= 0) | ~np.isfinite(merged["allocation_factor"])
if bad_alloc.any():
    print("Error: Found non-positive or non-finite allocation factors; resetting to 1.0")
    merged.loc[bad_alloc, "allocation_factor"] = 1.0

# This is the table we’ll use to create activities (kept name)
# ---------------------- ALLOCATION (SKIPPED) --------------------
# Not needed for this dataset; using product-level data directly
#corrected_df = process_product.copy()
#Using line 103

# ---------------------- CREATE NEW WORKING DB --------------------------
# Square-matrix QA (kept) :contentReference[oaicite:4]{index=4}
print(len(pd.unique(corrected_df["process_id"])))
print(len(pd.unique(corrected_df["exchange_flow_id"])))
if len(pd.unique(corrected_df["process_id"])) != len(pd.unique(corrected_df["exchange_flow_id"])):
    print("Error! _ Please correct for square matrix technosphere")
else:
    print("++++Test Passed++++")

user_database = USER_DB_PREFIX + str(random.randint(0, 100000))

# Fresh empty DB (same pattern) :contentReference[oaicite:5]{index=5}
try:
    del bw.databases[user_database]
except:
    pass
bw.Database(user_database).write({})
process_dict = {}
mydata_db = bw.Database(user_database)

# ---------------------- CREATE ACTIVITIES + PRODUCTION -----------------
corrected_df = corrected_df.reset_index(drop=True)

for index, row in corrected_df.iterrows():
    key = row["process_name"] + str(row["process_location"]) + row["process_id"]
    if key in process_dict:
        print(f"Warn: duplicate activity key {key}; exiting for safety")
        sys.exit(0)
    if "b1db8b2c" in row["process_id"]:
        print("Guard: bad process_id encountered; exiting")
        sys.exit(0)
    if row["process_location"] is np.nan:
        print("Guard: NaN process_location; exiting")
        sys.exit(0)

    # Create activity
    process_dict[key] = mydata_db.new_activity(
        code=row["process_id"],
        name=row["process_name"],
        unit=row["exchange_unit"],
        location=row["process_location"],
    )
    process_dict[key].save()

    # === Production exchange with allocation scaling (same logic) :contentReference[oaicite:6]{index=6}
    q_unalloc = abs(row["exchange_amount"])
    a = float(row.get("allocation_factor", 1.0))
    q_effective = q_unalloc / a

    process_dict[key].new_exchange(
        input=process_dict[key].key,
        name=row["exchange_flow_name"],
        amount=q_effective,
        unit=row["exchange_unit"],
        type="production",
        location=row["process_location"],
    ).save()
    process_dict[key]["reference product"] = row["exchange_flow_name"]
    process_dict[key]["production amount"] = q_effective
    process_dict[key]["unit"] = row["exchange_unit"]
    process_dict[key]["allocation factor"] = a
    process_dict[key]["allocation type"] = row.get("allocation_type", "UNKNOWN")
    process_dict[key].save()

    if index % 100 == 0:
        print(index)

# ----------------- INDEX CREATOR ------------------------
# Build a quick index: process_id -> activity object in the new DB
pid_to_activity = {}
for _, row in corrected_df.iterrows():
    k = row["process_name"] + row["process_location"] + row["process_id"]
    pid_to_activity[row["process_id"]] = process_dict[k]

# ------------------------ TECHNOSPHERE EXCHANGES -----------------------
print("*****=====Technosphere Exchanges======*****")
process_product_corrected_df = corrected_df.copy()
count = 0
process_product_corrected_df.sort_values(by="process_name", inplace=True)
# Collect unit mismatch issues for later audit
unit_issues = []
for index, row in process_product_corrected_df.iterrows():
    count += 1
    if count % 100 == 0:
        print(count)
    key = row["process_name"] + row["process_location"] + row["process_id"]

    # Clear any previous tech/bio exchanges (kept helper) :contentReference[oaicite:7]{index=7}
    _ = clear_technosphere_and_biosphere_exchanges(process_dict[key])

    # source technosphere rows for this consumer (same as your join logic) :contentReference[oaicite:8]{index=8}
    technosphere_df = test_data[test_data["process_id"] == row["_old_process_id"]]
    technosphere_df = technosphere_df[technosphere_df["exchange_is_input"] == True]
    technosphere_df = technosphere_df[technosphere_df["exchange_ecoinvent_type"] == "technosphere"]
    
    '''
    for _, n_row in technosphere_df.iterrows():
        temp_df = process_product_corrected_df[
            process_product_corrected_df["_old_process_id"] == n_row["exchange_supplying_process_id"]
        ]
        temp_df = temp_df[temp_df["_old_exchange_flow_id"] == n_row["exchange_flow_id"]].reset_index()  # :contentReference[oaicite:9]{index=9}
        info = n_row
        if len(temp_df) == 1:
            info = temp_df.iloc[0]
            # unit conversion kept identical pattern
            if info["exchange_unit"] == n_row["exchange_unit"]:
                amount = n_row["exchange_amount"]
            else:
                amount = convert_amount(
                    n_row["exchange_amount"],
                    n_row["exchange_unit"],
                    info["exchange_unit"],
                    n_row["exchange_flow_name"],
                )
            supplying_activity_key = info["process_name"] + info["process_location"] + info["process_id"]
            if supplying_activity_key == key:
                log_issue_tech(n_row, info, reason="Self-supply avoided (would corrupt diagonal)")
                continue

            process_dict[key].new_exchange(
                input=process_dict[supplying_activity_key].key,
                amount=abs(amount),
                name=n_row["exchange_flow_name"],
                location=None,
                unit=info["exchange_unit"],
                type="technosphere",
            ).save()
            
            
        else:
            if "Error:Cutoff dummy flow" in n_row["exchange_supplying_process_id"]:
                log_issue_tech(n_row, info, reason="Cutoff dummy flow skipped")
            else:
                log_issue_tech(n_row, info, reason="No supplying process/default provider")
        '''
    for _, n_row in technosphere_df.iterrows():
            # --- original path: try to resolve supplier inside the NEW DB you just built ---
            temp_df = process_product_corrected_df[
                (process_product_corrected_df["_old_process_id"] == n_row["exchange_supplying_process_id"]) &
                (process_product_corrected_df["_old_exchange_flow_id"] == n_row["exchange_flow_id"])
            ].reset_index(drop=True)
        
            supplier_act = None
            supplier_unit = None
            info = None  # keep for logging, set when we have an internal match
        
            if len(temp_df) == 1:
                # supplier exists among newly built activities (original behavior)
                info = temp_df.iloc[0]
                supplying_activity_key = info["process_name"] + info["process_location"] + info["process_id"]
                supplier_act = process_dict.get(supplying_activity_key)
                supplier_unit = info.get("exchange_unit")
        
            # --- NEW: fallback to existing USLCI DB by supplier UUID (exchange_supplying_process_id) ---
            if supplier_act is None:
                supp_pid = n_row["exchange_supplying_process_id"] or n_row["exchange_flow_id"]
                try:
                    supplier_act = bw.get_activity((USLCI_DB_NAME, supp_pid))  # <-- set USLCI_DB_NAME above
                    supplier_unit = supplier_act["unit"]
                except Exception:
                    supplier_act = None
        
            # If still not resolved, log & continue (original style)
            if supplier_act is None:
                if "Error:Cutoff dummy flow" in str(n_row["exchange_supplying_process_id"]):
                    log_issue_tech(n_row, info, reason="Cutoff dummy flow skipped")
                else:
                    log_issue_tech(n_row, info, reason="No supplying process/default provider (new DB + USLCI)")
                continue
        
            # Prevent self-loop (same as before)
            if supplier_act.key == process_dict[key].key:
                log_issue_tech(n_row, info, reason="Self-supply avoided (diagonal)")
                continue
        
            # Unit conversion (unchanged semantics)
            if supplier_unit and supplier_unit != n_row["exchange_unit"]:
                
                
                amount, new_unit = convert_amount_exchange_to_supplier(n_row["exchange_amount"],n_row["exchange_unit"],supplier_unit)
                if new_unit != supplier_unit:
                    unit_issues.append({
                        "consumer_process_id": row["process_id"],
                        "consumer_process_name": row["process_name"],
                        "exchange_flow_name": n_row["exchange_flow_name"],
                        "supplier_process_id": n_row["exchange_supplying_process_id"],
                        "supplier_unit": supplier_unit,
                        "exchange_unit": n_row["exchange_unit"]
                    })
                    continue

            else:
                amount = n_row["exchange_amount"]
                new_unit = supplier_unit
                
        
            # Create technosphere exchange (works for both new DB and USLCI fallback)
            process_dict[key].new_exchange(
                input=supplier_act.key,
                amount=abs(amount),
                name=n_row["exchange_flow_name"],
                location=None,
                unit=new_unit,
                type="technosphere",
            ).save()

flush_technosphere_issues()
if unit_issues:
    import pandas as pd
    unit_issues_df = pd.DataFrame(unit_issues)
    unit_issues_path = Path("uuid_audit") / "unit_mismatch_issues.csv"
    unit_issues_path.parent.mkdir(parents=True, exist_ok=True)
    unit_issues_df.to_csv(unit_issues_path, index=False)
    print(f"⚠️  Logged {len(unit_issues_df)} unit mismatch issues → {unit_issues_path}")

# ------------------------- BIOSPHERE EXCHANGES -------------------------
print("*****=====Biosphere Exchanges======*****")

# (same minimal local helper as in your file) :contentReference[oaicite:10]{index=10}
def _clear_biosphere_exchanges_only(act):
    removed = 0
    for exc in list(act.biosphere()):
        exc.delete()
        removed += 1
    act.save()
    return removed

bridge_df = pd.read_csv(BRIDGE_CSV)  # columns: uslci_id, biosphere_id (same as current) :contentReference[oaicite:11]{index=11}
biosphere = bw.Database("biosphere3")
bio_index = {act.get("code"): act for act in biosphere}  # fast lookup by biosphere code (same) :contentReference[oaicite:12]{index=12}

added, missing = 0, 0
process_product_corrected_df = corrected_df.copy()
count = 0

for _, row in process_product_corrected_df.iterrows():
    count += 1
    if count % 100 == 0:
        print(count)
    proc_key = row["process_name"] + row["process_location"] + row["process_id"]

    # Only OUT elementary flows; positive amounts (same filters) :contentReference[oaicite:13]{index=13}
    bios_df = test_data[
        (test_data["process_id"] == row["_old_process_id"])
        & (test_data["exchange_flow_type"] == "ELEMENTARY_FLOW")
    ].copy()
    bios_df = bios_df[bios_df["exchange_is_input"] == False]
    bios_df = bios_df[bios_df["exchange_amount"] > 0]
    if bios_df.empty:
        continue

    # Clear existing biosphere on this activity (kept)
    _ = _clear_biosphere_exchanges_only(process_dict[proc_key])

    for _, n_row in bios_df.iterrows():
        ex_id = str(n_row["exchange_flow_id"])
        match = bridge_df[bridge_df["uslci_id"] == ex_id]          # same match key name as your code :contentReference[oaicite:14]{index=14}
        if match.empty:
            missing += 1
            log_issue_bio(n_row, info=row, reason="No mapping in bridge", extra={"exchange_flow_id": ex_id})
            continue

        bio_code = str(match.iloc[0]["biosphere_id"])
        bio_flow = bio_index.get(bio_code)
        if bio_flow is None:
            missing += 1
            log_issue_bio(n_row, info=row, reason="Biosphere flow not found", extra={"biosphere_id": bio_code})
            continue

        try:
            process_dict[proc_key].new_exchange(
                input=bio_flow.key,
                amount=float(n_row["exchange_amount"]),
                type="biosphere",
                unit=bio_flow.get("unit"),
                name=n_row["exchange_flow_name"],
            ).save()
            added += 1
        except Exception as e:
            log_issue_bio(n_row, info=row, reason="Failed to add biosphere flow", extra={"error": str(e)})

print(f"[Biosphere] Added: {added}, Missing/Failed: {missing}")
flush_biosphere_issues()

# ---------------------------- LOCATION QA ------------------------------
# (verbatim structure, summarized) :contentReference[oaicite:15]{index=15}
mydata_db = bw.Database(user_database)
MAX_PER_DB = 10

def is_nan_like(x):
    if x is None: return False
    if isinstance(x, float) and math.isnan(x): return True
    if isinstance(x, str) and x.strip().lower() in {"nan", ""}: return True
    return False

def norm_key(loc):
    if loc is None: return None
    if isinstance(loc, (list, tuple)): return tuple(loc)
    return loc

nan_like_summary, unknown_summary = {}, {}
nan_like_examples, unknown_examples = {user_database: []}, {user_database: []}

for act in bw.Database(user_database):
    raw_loc = act.get("location")
    if is_nan_like(raw_loc):
        if len(nan_like_examples[user_database]) < MAX_PER_DB:
            nan_like_examples[user_database].append({"name": act.get("name"), "code": act.get("code"), "location": raw_loc})
        continue
    key = norm_key(raw_loc)
    if key is None: continue
    if key not in geomapping:
        if len(unknown_examples[user_database]) < MAX_PER_DB:
            unknown_examples[user_database].append({"name": act.get("name"), "code": act.get("code"), "location": key})

# Quick counts
if nan_like_examples[user_database]:
    nan_like_summary[user_database] = len(nan_like_examples[user_database])
if unknown_examples[user_database]:
    unknown_summary[user_database] = len(unknown_examples[user_database])

print("\n=== SUMMARY: NaN-like locations (will crash) ===")
print("None found ✅" if not nan_like_summary else nan_like_summary)

print("\n=== SUMMARY: Unknown locations (not in geomapping) ===")
print("None found ✅" if not unknown_summary else unknown_summary)

# ------------------------------ BACKUP --------------------------------
backup_file_path = backup_project_directory(project_name)
print(f"Project backed up to: {backup_file_path}")
