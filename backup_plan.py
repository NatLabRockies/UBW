import os
import pandas as pd
os.environ['BRIGHTWAY2_DIR']= "/Users/tghosh/Desktop/bw2uslci_test/"
import brightway2 as bw
import sys
from pathlib import Path
import csv
import random
import math
from bw2data import geomapping, config
import numpy as np
import warnings
from bw2io.backup import backup_project_directory
from helper_functions import flush_biosphere_issues,flush_technosphere_issues,log_issue_bio,log_issue_tech,convert_amount,debugger,clear_technosphere_and_biosphere_exchanges


# ------------------------------------------------------------------------------
# Output directory (only change to IO behavior): ensure a clean place for writes
# ------------------------------------------------------------------------------
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Suppress all warnings
warnings.filterwarnings("ignore")
pd.options.mode.chained_assignment = None  # turn off chained assignment warning

project_name = 'bw2uslci_generator_final'
bw.projects.set_current('bw2uslci_generator_final')
print(bw.databases)
process_dict = {}
bw.bw2setup()

# --- Load and prep USLCI data -------------------------------------------------
test_data= pd.read_csv('uslci.csv')
# Corrected - Removed all waste flows
test_data['process_location'] = test_data['process_location'].fillna("US")
test_data['exchange_supplying_process_id'] = test_data['exchange_supplying_process_id'].fillna("")
test_data['exchange_flow_type'] = test_data['exchange_type']
test_data['exchange_flow_name'] = test_data['exchange_name']
test_data['exchange_flow_id'] = test_data['exchange_id']
test_data['process_location'] = test_data['process_location'].replace(
    "Error:This process is missing location", 
    "US"
)
test_data['process_location'] = test_data['process_location'].replace(
    "United States of America (the)", 
    "US"
)
test_data['process_location'] = test_data['process_location'].replace(
    "Congo (the Democratic Republic of the)", 
    "Congo"
)
test_data = test_data[test_data['exchange_flow_type'] != "WASTE_FLOW"]
process_product = test_data[['process_name','process_id','process_location','process_description','exchange_flow_name','exchange_flow_id','exchange_amount','exchange_unit','exchange_ecoinvent_type']]
process_product = process_product[process_product['exchange_ecoinvent_type'] =='production']

# --- Allocation table load/validate -------------------------------------------
allocation_df = pd.read_csv('allocation_exchange_df.csv')
# If you want to pick a specific allocation type (e.g., PHYSICAL_ALLOCATION), filter here:
alloc_type = "PHYSICAL_ALLOCATION"

if alloc_type in set(allocation_df["allocation_type"].unique()):
    alloc_df_use = allocation_df[allocation_df["allocation_type"] == alloc_type].copy()
else:
    # Fall back to use what's present (e.g., CAUSAL_ALLOCATION) but keep label for traceability
    alloc_df_use = allocation_df.copy()
    print(f"Warning: '{alloc_type}' not found in CSV; using provided allocation_type(s):",
          sorted(alloc_df_use['allocation_type'].unique()))

# Keep only the keys we need + the factor
alloc_df_use = alloc_df_use.rename(columns={
    "exchange_val": "allocation_factor",   # clearer name
    "exchange_id": "exchange_flow_id"
})[["process_name", "process_id", "exchange_flow_id", "allocation_factor", "allocation_type"]]


def validate_allocation(
    corrected_df: pd.DataFrame,
    allocation_df: pd.DataFrame,
    alloc_type: str = "PHYSICAL_ALLOCATION",
    tol: float = 1e-6,
):
    """
    Validate allocation factors for split multi-output processes.
    """
    # --- Normalize/prepare CSV ---
    adf = allocation_df.rename(columns={
        "exchange_val": "allocation_factor",
        "exchange_id": "exchange_flow_id",
    }).copy()

    # Keep only relevant columns; preserve anything extra for traceability if present
    keep_cols = [c for c in ["process_id", "exchange_flow_id",
                             "allocation_factor", "allocation_type"] if c in adf.columns]
    adf = adf[keep_cols].copy()

    # Filter by alloc_type if available; otherwise proceed but note it
    requested_present = alloc_type in set(adf.get("allocation_type", pd.Series(dtype=object)).unique())
    if "allocation_type" in adf.columns and requested_present:
        adf = adf[adf["allocation_type"] == alloc_type].copy()

    # Types & cleaning
    adf["allocation_factor"] = pd.to_numeric(adf["allocation_factor"], errors="coerce")

    # --- Build the product list per original process from corrected_df ---
    # corrected_df rows are already only production rows
    prod_df = corrected_df[[
        "_old_process_id", "_old_exchange_flow_id",
        "process_id", "exchange_flow_id",
        "process_name", "exchange_flow_name",
        "exchange_amount", "exchange_unit"
    ]].drop_duplicates()

    # Rename to common keys for merge
    prod_df = prod_df.rename(columns={
        "_old_process_id": "orig_process_id",
        "_old_exchange_flow_id": "orig_exchange_id",
    })

    # For a clean merge, align CSV keys to the "orig_*" names
    adf = adf.rename(columns={
        "process_id": "orig_process_id",
        "exchange_flow_id": "orig_exchange_id",
    })

    # --- Merge allocation onto the product list (left: all products must be covered) ---
    merged = prod_df.merge(
        adf,
        how="inner",
        on=["orig_process_id", "orig_exchange_id"],
        validate="m:1"
    )

    # --- Detect problems row-wise ---
    issues = []

    # 1) Missing factor for a product
    missing_mask = merged["allocation_factor"].isna()
    for _, r in merged[missing_mask].iterrows():
        issues.append({
            "issue": "missing_factor",
            "orig_process_id": r["orig_process_id"],
            "orig_exchange_id": r["orig_exchange_id"],
            "process_name": r["process_name"],
            "exchange_flow_name": r["exchange_flow_name"]
        })

    # 2) Non-positive / non-finite factors
    bad_factor_mask = merged["allocation_factor"].notna() & (
        (merged["allocation_factor"] <= 0) | ~np.isfinite(merged["allocation_factor"])
    )
    for _, r in merged[bad_factor_mask].iterrows():
        issues.append({
            "issue": "invalid_factor",
            "orig_process_id": r["orig_process_id"],
            "orig_exchange_id": r["orig_exchange_id"],
            "process_name": r["process_name"],
            "exchange_flow_name": r["exchange_flow_name"],
            "allocation_factor": r["allocation_factor"]
        })

    # 3) Duplicates in CSV (same original (process, product) appearing >1 in CSV)
    dup_keys = adf.groupby(["orig_process_id", "orig_exchange_id"]).size()
    dup_keys = dup_keys[dup_keys > 1]
    for (pid, eid), cnt in dup_keys.items():
        issues.append({
            "issue": "duplicate_entry_in_csv",
            "orig_process_id": pid,
            "orig_exchange_id": eid,
            "count": int(cnt)
        })

    # 4) Extras in CSV (entries for products not present in prod_df)
    csv_only = adf.merge(
        prod_df[["orig_process_id", "orig_exchange_id"]],
        how="left",
        on=["orig_process_id", "orig_exchange_id"],
        indicator=True
    )
    csv_extras = csv_only[csv_only["_merge"] == "left_only"]
    for _, r in csv_extras.iterrows():
        issues.append({
            "issue": "extra_in_csv",
            "orig_process_id": r["orig_process_id"],
            "orig_exchange_id": r["orig_exchange_id"],
            "allocation_factor": r["allocation_factor"],
            "allocation_type": r.get("allocation_type")
        })

    # --- Per-process aggregation checks (sum≈1, coverage) ---
    grp = merged.groupby("orig_process_id", as_index=False).agg(
        n_products=("orig_exchange_id", "nunique"),
        n_allocated=("allocation_factor", lambda s: s.notna().sum()),
        sum_alloc=("allocation_factor", "sum"),
        min_alloc=("allocation_factor", "min"),
        max_alloc=("allocation_factor", "max"),
    )
    grp["sum_ok"] = grp["sum_alloc"].sub(1.0).abs() <= tol
    grp["coverage_ok"] = grp["n_allocated"] == grp["n_products"]

    for _, r in grp[~grp["sum_ok"]].iterrows():
        issues.append({
            "issue": "bad_sum",
            "orig_process_id": r["orig_process_id"],
            "n_products": int(r["n_products"]),
            "n_allocated": int(r["n_allocated"]),
            "sum_alloc": float(r["sum_alloc"])
        })
    for _, r in grp[~grp["coverage_ok"]].iterrows():
        issues.append({
            "issue": "incomplete_coverage",
            "orig_process_id": r["orig_process_id"],
            "n_products": int(r["n_products"]),
            "n_allocated": int(r["n_allocated"]),
        })

    grp["allocation_type_requested"] = alloc_type
    grp["allocation_type_present_in_csv"] = requested_present

    issues_df = pd.DataFrame(issues).sort_values(["issue", "orig_process_id"], ignore_index=True)
    summary_df = grp.sort_values("orig_process_id", ignore_index=True)
    merged_detail = merged.sort_values(["orig_process_id", "orig_exchange_id"], ignore_index=True)

    return summary_df, issues_df, merged_detail


# --- Split multi-output processes; uniquify IDs --------------------------------
clean_df = pd.DataFrame()
byproducts_df = pd.DataFrame()
for index,row in process_product.iterrows():
    find_dup =  process_product[(process_product['process_name'] == row['process_name']) & (process_product['process_id'] == row['process_id'])]
    if len(find_dup) > 1:
        find_dup = find_dup.copy()
        find_dup['_old_process_id'] = find_dup['process_id']
        find_dup['process_id'] = find_dup['process_id'] +"." + find_dup['exchange_flow_id']
        byproducts_df = pd.concat([byproducts_df,find_dup])
    else:
        find_dup = find_dup.copy()
        find_dup['_old_process_id'] = find_dup['process_id']
    clean_df = pd.concat([clean_df,find_dup])
clean_df = clean_df.drop_duplicates()

# Uniquify exchange IDs
correction_of_exchanges_df = clean_df.copy()
corrected_df = pd.DataFrame()
for index,row in correction_of_exchanges_df.iterrows():
    find_dup = correction_of_exchanges_df[(correction_of_exchanges_df['exchange_flow_id'] == row['exchange_flow_id'])]
    if len(find_dup) > 1:
        find_dup = find_dup.copy()
        find_dup['_old_exchange_flow_id'] = find_dup['exchange_flow_id']
        find_dup['exchange_flow_id'] = find_dup['process_id'] + "." +find_dup['exchange_flow_id']
    else:
        find_dup = find_dup.copy()
        find_dup['_old_exchange_flow_id'] = find_dup['exchange_flow_id']
    corrected_df = pd.concat([corrected_df,find_dup])
corrected_df = corrected_df.drop_duplicates()
save_corrected_df = corrected_df.copy()

# --- Validate allocation and write audit CSVs ----------------------------------
summary_df, issues_df, merged_detail = validate_allocation(
    corrected_df=corrected_df,
    allocation_df=allocation_df,
    alloc_type="PHYSICAL_ALLOCATION",
    tol=1e-6
)

print("=== Allocation Summary (per original process) ===")
print("\n=== Issues ===")
print(issues_df.head(50))

# Optional: write to CSVs for audit (→ output/)
summary_df.to_csv(OUTPUT_DIR / "allocation_summary.csv", index=False)
issues_df.to_csv(OUTPUT_DIR / "allocation_issues.csv", index=False)
merged_detail.to_csv(OUTPUT_DIR / "allocation_merged_detail.csv", index=False)

# --- Merge allocation onto corrected_df ---------------------------------------
del alloc_df_use['process_name']
corrected_df = corrected_df.merge(
    alloc_df_use.rename(columns={
        "process_id": "orig_process_id",
        "exchange_flow_id": "orig_exchange_id",
    }),
    how="left",
    left_on=["_old_process_id", "_old_exchange_flow_id"],
    right_on=["orig_process_id", "orig_exchange_id"],
    validate="m:1"
)

# Safety: clamp / clean
corrected_df["allocation_factor"] = corrected_df["allocation_factor"].astype(float)
corrected_df["allocation_factor"] = corrected_df["allocation_factor"].fillna(1.0)
bad_alloc = (corrected_df["allocation_factor"] <= 0) | ~np.isfinite(corrected_df["allocation_factor"])
if bad_alloc.any():
    print("Error: Found non-positive or non-finite allocation factors:")
    print(corrected_df.loc[bad_alloc, ["process_name","process_id","exchange_flow_id","allocation_factor"]].head(10))
    corrected_df.loc[bad_alloc, "allocation_factor"] = 1.0

# Save corrected/allocated process-product table (→ output/)
corrected_df.to_csv(OUTPUT_DIR / 'process_product_corrected_allocated.csv', index=False)
process_product_corrected_df = corrected_df.copy()

# Check square technosphere condition (counts only)
print(len(pd.unique(corrected_df['process_id'])))
print(len(pd.unique(corrected_df['exchange_flow_id'])))
if len(pd.unique(corrected_df['process_id'])) != len(pd.unique(corrected_df['exchange_flow_id'])):
    print("Error! _ Please correct for square matrix technosphere")
else:
    print("++++Test Passed++++")

user_database = "uslci_database"+str(random.randint(0,1000))

# --- Create/clear working database --------------------------------------------
try:
    del bw.databases[user_database]
except:
    pass
temp = bw.Database(user_database)
temp.write({})
process_dict = {}
mydata_db = bw.Database(user_database)
corrected_df = corrected_df.reset_index()

for index,row in corrected_df.iterrows():
    key = row['process_name'] + str(row['process_location']) + row['process_id']
    if key in process_dict:
        print(f"Warn: Key {key} already exists in process_dict. Overwriting activity..")
        sys.exit(0)
    if "b1db8b2c" in row['process_id']:
        print("The process with the issue is coming in")
        sys.exit(0)
    if row['process_location'] is np.nan:
        print("The process with the issue is coming in")
        sys.exit(0)
    process_dict[key] = mydata_db.new_activity(code = row['process_id'], name = row['process_name'], unit = row['exchange_unit'], location = row['process_location'])  
    process_dict[key].save()
    # === Allocation via production scaling ===
    q_unalloc = abs(row['exchange_amount'])
    a = float(row.get("allocation_factor", 1.0))
    q_effective = q_unalloc / a
    process_dict[key].new_exchange(input = process_dict[key].key, 
                                   name = row['exchange_flow_name'], 
                                   amount = q_effective,
                                   unit = row['exchange_unit'],
                                   type = 'production', 
                                   location = row['process_location']).save()
    process_dict[key]['reference product'] = row['exchange_flow_name']
    process_dict[key]['production amount'] = q_effective
    process_dict[key]['unit'] = row['exchange_unit']
    process_dict[key]['allocation factor'] = a
    process_dict[key]['allocation type'] = row.get('allocation_type', 'UNKNOWN')
    process_dict[key].save()
    if (index%100==0):
        print(index)



print("*****=====Technosphere Exchanges======*****")
process_product_corrected_df = corrected_df.copy()
# Adding Technosphere flows
count = 0
process_product_corrected_df.sort_values(by="process_name",inplace=True)
for index,row in process_product_corrected_df .iterrows():
    count += 1
    if count%100 == 0:
        print(count)
    key = row['process_name'] + row['process_location'] + row['process_id']
    cleared = clear_technosphere_and_biosphere_exchanges(process_dict[key])
    if cleared:
        pass
    technosphere_df = test_data[test_data['process_id'] == row['_old_process_id']]
    technosphere_df = technosphere_df[technosphere_df['exchange_is_input'] == True]
    technosphere_df = technosphere_df[technosphere_df['exchange_ecoinvent_type'] == "technosphere"]
    for n_index,n_row in technosphere_df.iterrows():
       temp_df = process_product_corrected_df[process_product_corrected_df['_old_process_id'] == n_row['exchange_supplying_process_id']]
       temp_df = temp_df[temp_df['_old_exchange_flow_id'] == n_row['exchange_flow_id']].reset_index()
       info = n_row
       if len(temp_df) == 1:
           info = temp_df.iloc[0]
           if info['exchange_unit'] == n_row['exchange_unit']:
               amount = n_row['exchange_amount']
           else:
               amount = convert_amount(n_row['exchange_amount'], 
                                        n_row['exchange_unit'], 
                                        info['exchange_unit'], 
                                        n_row['exchange_flow_name'])
           supplying_activity_key = info['process_name'] + info['process_location'] + info['process_id']
           if supplying_activity_key == key:
                log_issue_tech(n_row, info, reason="Self-supply avoided (would corrupt diagonal)")
                continue
           process_dict[key].new_exchange(
                input=process_dict[supplying_activity_key].key,
                amount=abs(amount),
                name=n_row['exchange_flow_name'],
                location=None,
                unit=info['exchange_unit'],
                type='technosphere'
           ).save()
       else:
            if "Error:Cutoff dummy flow" in n_row['exchange_supplying_process_id']:
                log_issue_tech(n_row, info, reason="Cutoff dummy flow skipped")
            else:
                log_issue_tech(n_row, info, reason="No supplying process/default provider")


flush_technosphere_issues()
# --- BIOSPHERE: add mapped elementary flows via bridge file --------------------
print("*****=====Biosphere Exchanges======*****")
def clear_biosphere_exchanges(act):
    """Delete all technosphere & biosphere exchanges on an activity; keep production."""
    removed = 0
    for exc in list(act.biosphere()):
        exc.delete()
        removed += 1
    act.save()
    return removed

BRIDGE_CSV = "working_bridge.csv"
bridge_df = pd.read_csv(BRIDGE_CSV)

added, missing = 0, 0
biosphere = bw.Database("biosphere3")
bio_index = {act.get("code"): act for act in biosphere}

process_product_corrected_df = corrected_df.copy()
count=0
for _, row in process_product_corrected_df.iterrows():
    proc = row
    count = count + 1
    if (count%100==0):
            print(count)
    proc_key = row["process_name"] + row["process_location"] + row["process_id"]
    bios_df = test_data[
        (test_data["process_id"] == row["_old_process_id"])
        & (test_data["exchange_flow_type"] == "ELEMENTARY_FLOW")
    ].copy()
    bios_df = bios_df[bios_df['exchange_is_input'] == False]
    bios_df = bios_df[bios_df['exchange_amount'] > 0]
    if bios_df.empty:
        continue
    for _, n_row in bios_df.iterrows():
        ex_id = str(n_row["exchange_flow_id"])
        match = bridge_df[bridge_df['uslci_id'] == ex_id]
        if match.empty:
            missing += 1
            log_issue_bio(n_row, info=proc, reason="No mapping in bridge", extra={"exchange_flow_id": ex_id})
            continue
        bio_code = str(match.iloc[0]['biosphere_id'])
        bio_flow = bio_index.get(bio_code)
        if bio_flow is None:
            missing += 1
            log_issue_bio(n_row, info=proc, reason="Biosphere flow not found", extra={"biosphere_id": bio_code})
            continue
        try:
            process_dict[proc_key].new_exchange(
                input=bio_flow.key,
                amount=float(n_row['exchange_amount']),
                type="biosphere",
                unit=bio_flow.get("unit"),
                name=n_row["exchange_flow_name"]
            ).save()
            added += 1
        except Exception as e:
            print('Failed')
            log_issue_bio(n_row, info=proc, reason="Failed to add biosphere flow", extra={"error": str(e)})

print(f"[Biosphere] Added: {added}, Missing/Failed: {missing}")
flush_biosphere_issues()

# --- Location QA ---------------------------------------------------------------
mydata_db = bw.Database(user_database)
MAX_PER_DB = 10

def is_nan_like(x):
    if x is None:
        return False
    if isinstance(x, float) and math.isnan(x):
        return True
    if isinstance(x, str) and x.strip().lower() in {"nan", ""}:
        return True
    return False

def norm_key(loc):
    """Normalize location to the key type Brightway expects."""
    if loc is None:
        return None
    if isinstance(loc, (list, tuple)):
        return tuple(loc)
    return loc  # string

nan_like_summary = {}
unknown_summary  = {}
nan_like_examples = {}
unknown_examples  = {}

db = bw.Database(user_database)
n_nan_like = n_unknown = 0
nan_like_examples[user_database] = []
unknown_examples[user_database]  = []

for act in db:
    raw_loc = act.get("location")
    if is_nan_like(raw_loc):
        n_nan_like += 1
        if len(nan_like_examples[user_database]) < MAX_PER_DB:
            nan_like_examples[user_database].append(
                {"name": act.get("name"), "code": act.get("code"), "location": raw_loc}
            )
        continue
    key = norm_key(raw_loc)
    if key is None:
        continue
    if key not in geomapping:
        n_unknown += 1
        if len(unknown_examples[user_database]) < MAX_PER_DB:
            unknown_examples[user_database].append(
                {"name": act.get("name"), "code": act.get("code"), "location": key}
            )

if n_nan_like:
    nan_like_summary[user_database] = n_nan_like
if n_unknown:
    unknown_summary[user_database] = n_unknown

print("\n=== SUMMARY: NaN-like locations (will crash) ===")
if not nan_like_summary:
    print("None found ✅")
else:
    for dbn, cnt in nan_like_summary.items():
        print(f"[{dbn}] {cnt}")

print("\n=== SUMMARY: Unknown locations (not in geomapping) ===")
if not unknown_summary:
    print("None found ✅")
else:
    for dbn, cnt in unknown_summary.items():
        print(f"[{dbn}] {cnt}")

print("\n=== EXAMPLES: NaN-like locations ===")
for dbn, rows in nan_like_examples.items():
    if rows:
        print(f"\n[{dbn}] (showing up to {MAX_PER_DB})")
        for r in rows:
            print(f"  - {r['name']} | code={r['code']} | location={r['location']}")

print("\n=== EXAMPLES: Unknown locations ===")
for dbn, rows in unknown_examples.items():
    if rows:
        print(f"\n[{dbn}] (showing up to {MAX_PER_DB})")
        for r in rows:
            print(f"  - {r['name']} | code={r['code']} | location={r['location']}")

MAX_SHOW = 20

def retupleize_local(x):
    # Brightway treats list-like locations as tuples
    if isinstance(x, list):
        return tuple(x)
    return x

bad = []

for act in bw.Database(user_database):
    act_dic = act.as_dict()
    raw_loc = act_dic['location']
    k = retupleize_local(raw_loc)
    use_key = k if k else config.global_location
    try:
        _ = geomapping[use_key]
    except KeyError:
        bad.append({
            "db": user_database,
            "name": act.get("name"),
            "code": act.get("code"),
            "raw_location": raw_loc,
            "retupleized": k,
            "used_key_repr": repr(use_key),
            "type_of_used_key": type(use_key).__name__,
        })

print(f"Bad locations found: {len(bad)}")
for row in bad[:MAX_SHOW]:
    print(row)

# --- Single-activity example LCA ----------------------------------------------
mydata_db = bw.Database(user_database)
activity = [act for act in mydata_db if "Electricity; at grid; consumption mix - US" in act['name']][0]
print(activity['name'])
functional_unit = {activity: 3.6}
method = list(bw.methods)[639]
print(method)

lca = bw.LCA(functional_unit,method=method)
lca.lci()
lca.lcia()
print( lca.score, activity)



# Set the current project you want to back up
# Backup the current project
backup_file_path = backup_project_directory(project_name)
print(f"Project backed up to: {backup_file_path}")


# --- Helper to save LCIA results (writes → output/) ---------------------------
def save_lcia_result_csv(activity, functional_unit, method, lca=None, out_path="lcia_result.csv"):
    """
    Save a tidy CSV with LCIA result and full context.
    Works with an existing `lca` or will compute it if not provided.
    Appends to the CSV if it already exists.
    """
    if lca is None:
        lca = bw.LCA(functional_unit, method=method)
        lca.lci()
        lca.lcia()

    act_db, act_code = activity.key
    row = {
        "activity_name": activity.get("name"),
        "activity_db": act_db,
        "activity_code": act_code,
        "reference_product": activity.get("reference product"),
        "activity_unit": activity.get("unit"),
        "location": activity.get("location"),
        "production_amount": activity.get("production amount"),
    }

    try:
        fu_amount = float(next(iter(functional_unit.values())))
    except Exception:
        fu_amount = np.nan
    row["functional_unit_amount"] = fu_amount

    m1, m2, m3 = method
    row.update({
        "method_1": m1,
        "method_2": m2,
        "method_3": m3,
        "method_str": str(method),
    })
    try:
        m_meta = getattr(bw.Method(method), "metadata", {}) or {}
        row["method_unit"] = m_meta.get("unit")
    except Exception:
        row["method_unit"] = None

    row["lcia_score"] = float(lca.score)

    out_file = Path(out_path)
    df = pd.DataFrame([row])
    if out_file.exists():
        df.to_csv(out_file, mode="a", header=False, index=False)
    else:
        df.to_csv(out_file, index=False)

    print(f"✅ Saved LCIA result for {row['activity_name']} → '{out_path}'")


# ---- Batch over all activities; write to output/lcia_results.csv -------------
mydata_db = bw.Database(user_database)
method = list(bw.methods)[639]
print("Using method:", method)

for activity in mydata_db:
    functional_unit = {activity: 1.0}
    try:
        save_lcia_result_csv(activity, functional_unit, method, out_path=OUTPUT_DIR / "lcia_results.csv")
    except Exception as e:
        print(f"Skipped {activity.get('name')} due to error: {e}")
