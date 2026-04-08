import os
from pathlib import Path
import pandas as pd
import numpy as np
import sys

# --- Configure your BW2 dir and project before running, or use env vars
os.environ['BRIGHTWAY2_DIR'] = "/Users/tghosh/Library/Application Support/Brightway3"

import brightway2 as bw

# ======= USER INPUTS =======
PROJECT_NAME   = "uslci_transfer_activities"
SOURCE_DB_NAME = "N-SCITE"  # the hand-crafted database you want to extract
OUTPUT_CSV     = "extracted_handcrafted_dataset.csv"
# ===========================

def safe(x, default=None):
    return x if x is not None else default

def get_ref_product_name(act):
    # Prefer explicit reference product, fallback to activity name
    return act.get("reference product") or act.get("name")

def get_production_amount(act):
    # Prefer metadata, else try to read first production exchange amount, else 1.0
    pa = act.get("production amount")
    if isinstance(pa, (int, float)) and np.isfinite(pa) and pa != 0:
        return float(pa)
    prods = [e for e in act.exchanges() if e['type'] == 'production']
    if prods:
        try:
            return float(prods[0]['amount'])
        except Exception:
            pass
    return 1.0

def row_base_for_activity(act):
    return {
        "process_name": act.get("name"),
        "process_id": act.get("code"),
        "process_location": act.get("location") or "US",  # default US to match your cleaning
        "process_description": act.get("comment"),
    }

def export_database(db_name: str) -> pd.DataFrame:
    db = bw.Database(db_name)
    rows = []

    # -------- PRODUCTION rows (one per activity) --------
    for act in db:
        base = row_base_for_activity(act)
        ref_name = get_ref_product_name(act)
        unit = act.get("unit")

        rows.append({
            **base,
            # production exchange fields
            "exchange_name": ref_name,
            "exchange_id": act.get("code"),                 # use activity code as the product id
            "exchange_flow_name": ref_name,                 # convenience copy (your pipeline uses this)
            "exchange_flow_id": act.get("code"),
            "exchange_amount": get_production_amount(act),
            "exchange_unit": unit,
            "exchange_type": "PRODUCT_FLOW",
            "exchange_category": None,
            "exchange_description": None,
            "exchange_is_input": False,
            "exchange_ecoinvent_type": "production",
            # supplying info (empty for production)
            "exchange_supplying_process_id": "",
            "exchange_supplying_process_name": "",
            "exchange_supplying_process_location": "",
        })

    # -------- TECHNOSPHERE rows (inputs to each activity) --------
    for act in db:
        base = row_base_for_activity(act)
        for exc in act.technosphere():
            try:
                prov = bw.get_activity(exc.input)
            except Exception:
                prov = None

            prov_code = prov.get("code") if prov else None
            prov_name = prov.get("name") if prov else None
            prov_loc  = prov.get("location") if prov else None
            prov_refp = get_ref_product_name(prov) if prov else None
            prov_unit = prov.get("unit") if prov else None

            rows.append({
                **base,
                "exchange_name": prov_refp or exc.get("name"),
                # IMPORTANT: use provider activity code as the flow id (consistent with your matching)
                "exchange_id": prov_code,
                "exchange_flow_name": prov_refp or exc.get("name"),
                "exchange_flow_id": prov_code,
                "exchange_amount": float(abs(exc.get("amount", 0.0))),
                "exchange_unit": prov_unit or exc.get("unit"),
                "exchange_type": "TECHNOSPHERE",
                "exchange_category": None,
                "exchange_description": None,
                "exchange_is_input": True,
                "exchange_ecoinvent_type": "technosphere",
                "exchange_supplying_process_id": prov_code or "",
                "exchange_supplying_process_name": prov_name or "",
                "exchange_supplying_process_location": prov_loc or "",
            })

    # -------- BIOSPHERE rows (elementary flows) --------
    # Convention: emissions/output elementary flows → not inputs
    for act in db:
        base = row_base_for_activity(act)
        for exc in act.biosphere():
            bio_flow = None
            try:
                bio_flow = bw.get_activity(exc.input)
            except Exception:
                pass

            flow_name = bio_flow.get("name") if bio_flow else exc.get("name")
            flow_unit = bio_flow.get("unit") if bio_flow else exc.get("unit")
            flow_code = bio_flow.get("code") if bio_flow else None
            flow_cat  = "; ".join(bio_flow.get("categories", [])) if (bio_flow and bio_flow.get("categories")) else None

            amount = float(exc.get("amount", 0.0))
            if amount <= 0:
                # keep positive-outflow convention to match your downstream filters
                continue

            rows.append({
                **base,
                "exchange_name": flow_name,
                "exchange_id": flow_code,
                "exchange_flow_name": flow_name,
                "exchange_flow_id": flow_code,
                "exchange_amount": amount,
                "exchange_unit": flow_unit,
                "exchange_type": "ELEMENTARY_FLOW",
                "exchange_category": flow_cat,
                "exchange_description": None,
                "exchange_is_input": False,
                "exchange_ecoinvent_type": "biosphere",
                # no provider for elementary flows
                "exchange_supplying_process_id": "",
                "exchange_supplying_process_name": "",
                "exchange_supplying_process_location": "",
            })

    df = pd.DataFrame(rows)

    # Post-clean to match your downstream expectations
    df["process_location"] = df["process_location"].fillna("US")
    # Mirror the pattern in your pipeline (exchange_flow_type duplicates exchange_type)
    df["exchange_flow_type"] = df["exchange_type"]

    # Write CSV
    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Wrote {len(df):,} rows → {OUTPUT_CSV}")
    return df

if __name__ == "__main__":
    bw.projects.set_current(PROJECT_NAME)
    export_database(SOURCE_DB_NAME)
