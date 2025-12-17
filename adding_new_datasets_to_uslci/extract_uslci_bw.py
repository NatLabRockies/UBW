# extract_bw_db_to_csv_uslci.py
import os
from pathlib import Path
import pandas as pd
import numpy as np

# os.environ['BRIGHTWAY2_DIR'] = "/path/to/your/bw2/dir"
import brightway2 as bw

# ======= USER INPUTS =======
PROJECT_NAME   = "bw2uslci_generator_final"
SOURCE_DB_NAME = "uslci_database596"  # <- point this at your USLCI DB name in BW2
OUTPUT_CSV     = "completed_uslci_dataset.csv"
# ===========================

def safe(x, default=None):
    return x if x is not None else default

def get_ref_product_name(act):
    return act.get("reference product") or act.get("name")

def get_production_amount(act):
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
        "process_location": act.get("location") or "US",
        "process_description": act.get("comment"),
    }

def export_database(db_name: str) -> pd.DataFrame:
    db = bw.Database(db_name)
    rows = []

    # -------- PRODUCTION rows --------
    for act in db:
        base = row_base_for_activity(act)
        ref_name = get_ref_product_name(act)
        unit = act.get("unit")

        rows.append({
            **base,
            "exchange_name": ref_name,
            "exchange_id": act.get("code"),
            "exchange_flow_name": ref_name,
            "exchange_flow_id": act.get("code"),
            "exchange_amount": get_production_amount(act),
            "exchange_unit": unit,
            "exchange_type": "PRODUCT_FLOW",
            "exchange_category": None,
            "exchange_description": None,
            "exchange_is_input": False,
            "exchange_ecoinvent_type": "production",
            "exchange_supplying_process_id": "",
            "exchange_supplying_process_name": "",
            "exchange_supplying_process_location": "",
        })

    # -------- TECHNOSPHERE rows --------
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

    # -------- BIOSPHERE rows --------
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
                "exchange_supplying_process_id": "",
                "exchange_supplying_process_name": "",
                "exchange_supplying_process_location": "",
            })

    df = pd.DataFrame(rows)
    df["process_location"] = df["process_location"].fillna("US")
    df["exchange_flow_type"] = df["exchange_type"]

    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Wrote {len(df):,} rows → {OUTPUT_CSV}")
    return df

if __name__ == "__main__":
    bw.projects.set_current(PROJECT_NAME)
    export_database(SOURCE_DB_NAME)
