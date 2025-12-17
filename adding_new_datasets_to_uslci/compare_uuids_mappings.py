# compare_technosphere_suppliers_between_extractions.py
from pathlib import Path
import pandas as pd

# ================== USER INPUTS ==================
HANDCRAFTED_EXTRACT_CSV = "extracted_handcrafted_dataset.csv"  # from your hand-crafted DB
USLCI_EXTRACT_CSV       = "completed_uslci_dataset.csv"        # from the USLCI extractor above
OUT_DIR                 = Path("uuid_audit")
OUT_FILE                = OUT_DIR / "technosphere_missing_suppliers_vs_uslci.csv"
SUGGEST_FILE            = OUT_DIR / "technosphere_supplier_suggestions.csv"
WRITE_SUGGESTIONS       = True
# =================================================

OUT_DIR.mkdir(parents=True, exist_ok=True)

def _norm(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # columns already match by construction, but keep defensive normalization
    if "exchange_flow_name" not in df.columns and "exchange_name" in df.columns:
        df["exchange_flow_name"] = df["exchange_name"]
    if "exchange_flow_id" not in df.columns and "exchange_id" in df.columns:
        df["exchange_flow_id"] = df["exchange_id"]
    if "process_location" in df.columns:
        df["process_location"] = df["process_location"].fillna("US")
    if "exchange_supplying_process_id" not in df.columns and "exchange_id" in df.columns:
        df["exchange_supplying_process_id"] = df["exchange_id"]
    return df

def _only_technosphere(df: pd.DataFrame) -> pd.DataFrame:
    if "exchange_ecoinvent_type" in df.columns:
        return df[df["exchange_ecoinvent_type"].str.lower() == "technosphere"].copy()
    return df[df["exchange_type"].str.upper() == "TECHNOSPHERE"].copy()

def _supplier_catalog(uslci_df: pd.DataFrame):
    """
    Build the set of valid supplier process_ids from the USLCI extract,
    and a helper table for suggestions keyed by (process_name, location).
    """
    # Any activity with a production row exists as supplier; use all process_ids present
    if "process_id" not in uslci_df.columns:
        return set(), pd.DataFrame()
    supplier_ids = set(uslci_df["process_id"].dropna().astype(str).unique())

    # Build a small lookup for suggestions by (name, location)
    sugg = (
        uslci_df[["process_id","process_name","process_location"]]
        .drop_duplicates()
        .assign(process_location=lambda d: d["process_location"].fillna("US"))
    )
    return supplier_ids, sugg

def update_supplier_ids_from_suggestions(df, suggestions_df):
    """
    Update supplier IDs in-place for technosphere rows using the mapping
    provided in suggestions_df (output of the comparison step).

    This modifies:
      - exchange_supplying_process_id
      - exchange_id
      - exchange_flow_id
    so they all reference the new supplier UUIDs.

    Parameters
    ----------
    df : pandas.DataFrame
        The extracted hand-crafted dataset (with technosphere rows).
    suggestions_df : pandas.DataFrame
        The suggestions table produced by the comparison script.
        Must contain either:
        - 'old_supplier_process_id' & 'suggested_new_supplier_process_id', or
        - 'from_supplier_process_id' & 'to_supplier_process_id'
    Returns
    -------
    pandas.DataFrame
        Updated DataFrame (also modified in place).
    """

    # Determine correct column names for mapping
    if {"old_supplier_process_id", "suggested_new_supplier_process_id"}.issubset(suggestions_df.columns):
        old_col, new_col = "old_supplier_process_id", "suggested_new_supplier_process_id"
    elif {"from_supplier_process_id", "to_supplier_process_id"}.issubset(suggestions_df.columns):
        old_col, new_col = "from_supplier_process_id", "to_supplier_process_id"
    else:
        raise ValueError("Suggestions file must contain expected column pairs "
                         "('old_supplier_process_id'/'suggested_new_supplier_process_id') "
                         "or ('from_supplier_process_id'/'to_supplier_process_id').")

    # Build mapping dictionary
    mapping = (
        suggestions_df.dropna(subset=[old_col, new_col])
        .drop_duplicates(subset=[old_col])
        .set_index(old_col)[new_col]
        .astype(str)
        .to_dict()
    )

    # Identify technosphere rows
    if "exchange_ecoinvent_type" in df.columns:
        tech_mask = df["exchange_ecoinvent_type"].str.lower() == "technosphere"
    else:
        tech_mask = df["exchange_type"].str.upper() == "TECHNOSPHERE"

    # Apply mapping
    updated = 0
    for idx in df[tech_mask].index:
        old_id = str(df.at[idx, "exchange_supplying_process_id"])
        if old_id in mapping:
            new_id = mapping[old_id]
            for col in ["exchange_supplying_process_id", "exchange_id", "exchange_flow_id"]:
                if col in df.columns:
                    df.at[idx, col] = new_id
            updated += 1

    print(f"✅ Updated supplier UUIDs in {updated} technosphere rows.")
    return df


def apply_unit_bridge_to_df(final_df: pd.DataFrame,
                            bridge_csv: str,
                            out_csv: str,
                            issues_csv: str) -> pd.DataFrame:
    """
    Apply a unit name bridge to `exchange_unit` in the final dataframe and write outputs.

    Parameters
    ----------
    final_df : pd.DataFrame
        Your final dataframe (e.g., the extracted/mapped CSV already loaded).
        Must have column 'exchange_unit'.
    bridge_csv : str
        Path to CSV with columns ['unit','unit_new'].
    out_csv : str
        Path to write the updated dataframe.
    issues_csv : str
        Path to write units that weren't found in the bridge (with counts).

    Returns
    -------
    pd.DataFrame
        The updated dataframe (also written to out_csv).
    """
    df = final_df.copy()

    # Safety: ensure column present
    if "exchange_unit" not in df.columns:
        raise KeyError("final_df is missing required column 'exchange_unit'")

    # Load bridge → mapping (case/space-insensitive on the left side)
    bridge = pd.read_csv(bridge_csv)
    if not {"unit", "unit_new"}.issubset(bridge.columns):
        raise KeyError("Bridge CSV must have columns: 'unit' and 'unit_new'")

    bridge = bridge.dropna(subset=["unit", "unit_new"]).copy()
    bridge["unit_norm"] = bridge["unit"].astype(str).str.strip().str.lower()
    bridge_map = dict(zip(bridge["unit_norm"], bridge["unit_new"].astype(str)))

    # Normalize helper
    norm = lambda s: " ".join(str(s).strip().lower().split()) if pd.notna(s) else s

    # Determine which units in df are mapped vs missing
    df["__unit_norm__"] = df["exchange_unit"].apply(norm)
    unique_units = df["__unit_norm__"].dropna().unique().tolist()

    missing_units = sorted(u for u in unique_units if u not in bridge_map)
    if missing_units:
        # Count occurrences of missing units
        miss_counts = (
            df[df["__unit_norm__"].isin(missing_units)]
            .groupby("__unit_norm__", dropna=False, as_index=False)
            .size()
            .rename(columns={"__unit_norm__": "unit", "size": "occurrences"})
        )
        Path(issues_csv).parent.mkdir(parents=True, exist_ok=True)
        miss_counts.to_csv(issues_csv, index=False)
        print(f"⚠️  {len(missing_units)} unit(s) not found in bridge → {issues_csv}")
    else:
        print("✅ All units found in bridge; no issues file written.")

    # Apply mapping (only where present)
    def map_unit(u_raw):
        u_norm = norm(u_raw)
        return bridge_map.get(u_norm, u_raw)

    before = df["exchange_unit"].copy()
    df["exchange_unit"] = df["exchange_unit"].apply(map_unit)

    # Simple change summary
    changed = (before != df["exchange_unit"])
    print(f"🔁 Units updated in {changed.sum()} rows "
          f"({changed.mean():.1%} of dataframe).")

    # Clean up helper column and write
    df = df.drop(columns=["__unit_norm__"])
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"💾 Wrote unit-updated dataframe → {out_csv}")

    return df

def main():
    hand = _norm(pd.read_csv(HANDCRAFTED_EXTRACT_CSV, low_memory=False))
    uslci = _norm(pd.read_csv(USLCI_EXTRACT_CSV,       low_memory=False))

    hand_tech = _only_technosphere(hand)
    supplier_ids_new, sugg_table = _supplier_catalog(uslci)

    # Coerce to str for set checks
    hand_tech["exchange_supplying_process_id"] = hand_tech["exchange_supplying_process_id"].astype(str)

    missing = []
    suggestions = []

    for _, r in hand_tech.iterrows():
        supplier_id = r["exchange_supplying_process_id"]
        if supplier_id in supplier_ids_new:
            continue  # present in new USLCI extract

        # record the missing supplier link
        missing.append({
            "consumer_process_id": r["process_id"],
            "consumer_process_name": r["process_name"],
            "supplier_process_id": supplier_id,
            "supplier_process_name": r.get("exchange_supplying_process_name"),
            "supplier_location": r.get("exchange_supplying_process_location"),
            "input_flow_name": r.get("exchange_flow_name"),
            "input_flow_unit": r.get("exchange_unit"),
            "note": "Supplier process_id not found in new USLCI extract",
        })

        # optional: suggest a replacement by (name, location) if we have a name
        if WRITE_SUGGESTIONS:
            sp_name = r.get("exchange_supplying_process_name")
            sp_loc  = r.get("exchange_supplying_process_location") or "US"
            cand = sugg_table
            if sp_name:
                cand = cand[cand["process_name"] == sp_name]
            if "process_location" in cand.columns and sp_loc:
                cand = cand[cand["process_location"] == sp_loc]
            # take the first exact (name+loc) match, else leave blank
            if not cand.empty:
                new_pid = cand.iloc[0]["process_id"]
                suggestions.append({
                    "old_supplier_process_id": supplier_id,
                    "old_supplier_process_name": sp_name,
                    "old_supplier_location": sp_loc,
                    "suggested_new_supplier_process_id": new_pid,
                    "match_basis": "name+location" if sp_name else "location_only",
                })

    missing_df = pd.DataFrame(missing).drop_duplicates()
    missing_df.to_csv(OUT_FILE, index=False)
    print(f"🔎 Missing suppliers vs USLCI: {len(missing_df)} → {OUT_FILE}")

    if WRITE_SUGGESTIONS:
        sugg_df = pd.DataFrame(suggestions).drop_duplicates()
        sugg_df.to_csv(SUGGEST_FILE, index=False)
        print(f"🧭 Suggestions: {len(sugg_df)} → {SUGGEST_FILE}")
        # assuming you've already got these loaded in Spyder:
        df = pd.read_csv("extracted_handcrafted_dataset.csv")
        suggestions_df = pd.read_csv("uuid_audit/technosphere_supplier_suggestions.csv")
        
        df = update_supplier_ids_from_suggestions(df, suggestions_df)
        
        updated_df = apply_unit_bridge_to_df(
        final_df=df,
        bridge_csv="unit_bridge.csv",                     # your bridge file (unit, unit_new)
        out_csv="final_df_units_mapped.csv",              # new output file
        issues_csv="final_df_unit_missing_issues.csv"     # units not found in bridge
)
        
        # then optionally save:
        updated_df.to_csv("extracted_handcrafted_dataset_mapped.csv", index=False)




print("💾 Updated file saved!")
if __name__ == "__main__":
    main()
