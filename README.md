# USLCI → Brightway2 Inventory Builder

This repository provides a pipeline for **converting the USLCI database into a Brightway2-compatible format** and for **adding custom/hand-crafted LCA datasets into an existing USLCI Brightway2 project**. The two workflows are independent and can be used together.

---

## Repository Structure

```
.
├── uslci.csv                        # Raw USLCI input data (CSV export from openLCA/FLCAC)
├── working_bridge.csv               # Maps USLCI elementary flow UUIDs → biosphere3 UUIDs
├── allocation_exchange_df.csv       # Allocation factors for multi-output processes
├── process_product_corrected_allocated.csv  # Intermediate: cleaned, allocated process-product table
├── __init__.py
│
├── project/
│   ├── load_project_ab.py           # Restore a pre-built USLCI BW2 project from backup
│   └── uslci_elci_brightway2.tar.gz # Pre-built BW2 project archive (ready to restore)
│
├── adding_new_datasets_to_uslci/
│   ├── helper_functions.py          # Shared utilities: issue logging, unit conversions, exchange helpers
│   ├── bw_db_to_csv.py              # Export a custom/hand-crafted BW2 database to CSV
│   ├── extract_uslci_bw.py          # Export the full USLCI BW2 database to CSV
│   ├── compare_uuids_mappings.py    # Audit technosphere supplier UUIDs; find mismatches vs. USLCI
│   ├── push_extracted_dataset_bw2.py# Push a mapped CSV dataset into BW2 as a new database
│   ├── unit_extractor.py            # Extract unique unit strings from a BW2 database
│   ├── unit_bridge.csv              # Unit mapping table used during extraction/push
│   ├── extracted_handcrafted_dataset.csv         # Raw extract from hand-crafted BW2 DB
│   ├── extracted_handcrafted_dataset_mapped.csv  # UUID-corrected version (input to push step)
│   ├── completed_uslci_dataset.csv               # Full USLCI extract used for UUID auditing
│   └── uuid_audit/
│       ├── technosphere_missing_suppliers_vs_uslci.csv
│       └── technosphere_supplier_suggestions.csv
│
└── output/
    ├── process_product_corrected_allocated.csv
    ├── lcia_results.csv
    ├── exchange_issues.csv
    ├── technosphere_issues.csv
    ├── biosphere_issues.csv
    ├── allocation_issues.csv
    ├── allocation_merged_detail.csv
    ├── allocation_summary.csv
    ├── elementary_flows.csv
    ├── biosphere3_export.csv
    └── ReCiPe_characterization_factors.csv
```

---

## Prerequisites

### Python Environment

Create a dedicated conda environment:

```bash
conda create -n bw2uslci python=3.11
conda activate bw2uslci
pip install brightway2 bw2io numpy pandas
```

> **Note:** All scripts in this repo set `BRIGHTWAY2_DIR` explicitly in code (e.g., `"/Users/tghosh/Desktop/bw2uslci_test/"`). Before running any script, update this path to point to your own Brightway2 data directory, or set it as an environment variable in your shell:
>
> ```bash
> export BRIGHTWAY2_DIR="/your/path/to/brightway2_data/"
> ```

---

## Workflow A — Restore the Pre-Built USLCI Project (Quickest Start)

If you want to skip building the database from scratch, a pre-built Brightway2 project backup is included.

**Script:** `project/load_project_ab.py`

```python
import os, brightway2 as bw
from bw2io.backup import restore_project_directory
restore_project_directory("uslci_elci_brightway2.tar.gz")
```

**Steps:**

1. Ensure `project/uslci_elci_brightway2.tar.gz` is present.
2. Open a Python session (or Jupyter notebook) with `bw2uslci` activated.
3. Update `BRIGHTWAY2_DIR` in your environment or at the top of the script.
4. Run:
   ```bash
   cd project/
   python load_project_ab.py
   ```
5. After the restore, you can open the project:
   ```python
   import brightway2 as bw
   bw.projects.set_current("bw2uslci_generator_final")
   print(bw.databases)
   ```

---

## Workflow B — Add a Custom Dataset to the Existing USLCI Project

This is the primary active workflow. It lets you take a hand-crafted database already loaded in Brightway2 (e.g., built in Activity Browser), extract it to a portable CSV format, audit its UUID linkages against USLCI, fix any broken supplier references, and then push the corrected dataset back into the shared project.

All scripts live in `adding_new_datasets_to_uslci/`. Run them from inside that directory:

```bash
cd adding_new_datasets_to_uslci/
```

---

### Step 1 — Export the USLCI Database to CSV

**Script:** `extract_uslci_bw.py`

Edit the user inputs at the top of the file:

```python
PROJECT_NAME   = "bw2uslci_generator_final"   # your BW2 project
SOURCE_DB_NAME = "uslci_database596"           # the USLCI database name in that project
OUTPUT_CSV     = "completed_uslci_dataset.csv"
```

Run:

```bash
python extract_uslci_bw.py
```

This writes `completed_uslci_dataset.csv` — a flat table of all USLCI processes and their exchanges. It is used as the reference catalog in Step 3.

---

### Step 2 — Export Your Custom/Hand-Crafted Database to CSV

**Script:** `bw_db_to_csv.py`

Edit the user inputs:

```python
PROJECT_NAME   = "uslci_transfer_activities"   # project containing your hand-crafted DB
SOURCE_DB_NAME = "N-SCITE"                     # your custom database name
OUTPUT_CSV     = "extracted_handcrafted_dataset.csv"
```

Also update `BRIGHTWAY2_DIR` at the top of the file to match your setup.

Run:

```bash
python bw_db_to_csv.py
```

This writes `extracted_handcrafted_dataset.csv`.

---

### Step 3 — Audit Technosphere Supplier UUIDs

**Script:** `compare_uuids_mappings.py`

This script compares the technosphere inputs in your hand-crafted dataset against the supplier IDs available in the USLCI export. It flags any suppliers that are missing or use incorrect UUIDs.

Edit the user inputs:

```python
HANDCRAFTED_EXTRACT_CSV = "extracted_handcrafted_dataset.csv"
USLCI_EXTRACT_CSV       = "completed_uslci_dataset.csv"
OUT_DIR                 = Path("uuid_audit")
OUT_FILE                = OUT_DIR / "technosphere_missing_suppliers_vs_uslci.csv"
SUGGEST_FILE            = OUT_DIR / "technosphere_supplier_suggestions.csv"
WRITE_SUGGESTIONS       = True
```

Run:

```bash
python compare_uuids_mappings.py
```

Outputs written to `uuid_audit/`:
- `technosphere_missing_suppliers_vs_uslci.csv` — rows with suppliers not found in USLCI
- `technosphere_supplier_suggestions.csv` — suggested USLCI matches based on name/location lookup

Review both files and manually correct UUIDs in `extracted_handcrafted_dataset.csv`, saving the result as `extracted_handcrafted_dataset_mapped.csv`.

---

### Step 4 — Push the Corrected Dataset into Brightway2

**Script:** `push_extracted_dataset_bw2.py`

Edit the user inputs at the top:

```python
PROJECT_NAME      = "bw2uslci_generator_final"          # target BW2 project (must already exist)
INPUT_DATASET_CSV = "extracted_handcrafted_dataset_mapped.csv"  # UUID-corrected CSV from Step 3
ALLOCATION_CSV    = "allocation_exchange_df.csv"         # allocation factors (from repo root or custom)
BRIDGE_CSV        = "working_bridge.csv"                 # USLCI ↔ biosphere3 bridge (from repo root)
ALLOC_TYPE        = "PHYSICAL_ALLOCATION"                # allocation type to apply
USER_DB_PREFIX    = "my_dataset_db_"                     # new DB will be named with this prefix + random int
USLCI_DB_NAME     = "uslci_database596"                  # name of the existing USLCI DB in the project
```

Run:

```bash
python push_extracted_dataset_bw2.py
```

This will:
1. Load and clean the input CSV
2. Apply allocation factors
3. Create a new Brightway2 database (named `my_dataset_db_<random>`)
4. Add all activities and production exchanges
5. Add technosphere and biosphere exchanges
6. Write issue logs to `output/` (technosphere and biosphere issues)

---

### Optional — Extract Unit Strings from a BW2 Database

**Script:** `unit_extractor.py`

Useful when you need to audit units before building a unit bridge. Edit the project and database name at the top, then run:

```bash
python unit_extractor.py
```

Writes `uslci_old_units.csv` with all unique unit strings found.

---

## Key Input Files

| File | Description |
|------|-------------|
| `uslci.csv` | Full USLCI dataset in flat CSV format (process + exchange rows). Used by the core BW2-builder pipeline. |
| `working_bridge.csv` | Two-column CSV mapping `uslci_id` → `biosphere_id` (biosphere3 UUIDs). Required for biosphere exchange linking. |
| `allocation_exchange_df.csv` | Multi-output allocation table with columns: `process_name`, `process_id`, `exchange_name`, `exchange_val`, `exchange_id`, `allocation_type`. |
| `adding_new_datasets_to_uslci/unit_bridge.csv` | Unit harmonization table for the custom dataset push workflow. |

---

## Output Files

All outputs are written to the `output/` directory:

| File | Description |
|------|-------------|
| `process_product_corrected_allocated.csv` | Cleaned and allocated process-product table |
| `lcia_results.csv` | LCIA scores for all activities (method, score, unit) |
| `exchange_issues.csv` | General exchange linking issues |
| `technosphere_issues.csv` | Technosphere linkage failures (missing suppliers, unit mismatches, etc.) |
| `biosphere_issues.csv` | Biosphere linkage failures (unmapped flows, etc.) |
| `allocation_issues.csv` | Processes with invalid or missing allocation factors |
| `allocation_merged_detail.csv` | Detailed allocation merge diagnostics |
| `allocation_summary.csv` | Summary of allocation coverage |
| `elementary_flows.csv` | All USLCI elementary flows |
| `biosphere3_export.csv` | Full biosphere3 database export for reference |
| `ReCiPe_characterization_factors.csv` | Exported ReCiPe characterization factors |

---

## Brightway2 Project Names Used in This Repo

| Script | Project Name | Database Name |
|--------|-------------|---------------|
| `project/load_project_ab.py` | restored from backup | restored from backup |
| `extract_uslci_bw.py` | `bw2uslci_generator_final` | `uslci_database596` |
| `bw_db_to_csv.py` | `uslci_transfer_activities` | `N-SCITE` |
| `push_extracted_dataset_bw2.py` | `bw2uslci_generator_final` | `my_dataset_db_<N>` (new) |

Update these names in each script to match your local Brightway2 setup.

---

## Troubleshooting

**`BRIGHTWAY2_DIR` errors** — Each script sets this path explicitly near the top. Make sure it points to a directory that exists on your machine before running.

**Missing suppliers in technosphere** — Run `compare_uuids_mappings.py` (Step 3) and check `uuid_audit/` outputs. Supplier UUIDs in your hand-crafted dataset need to match those in the USLCI database.

**Unit mismatch errors** — The `flow_conversions` dictionary in `helper_functions.py` handles known unit pairs. If a new flow has mismatched units, add a conversion entry there.

**Square-matrix check fails** — `push_extracted_dataset_bw2.py` validates that the number of unique `process_id` values equals the number of unique `exchange_flow_id` values. Each process must have exactly one reference product.

**Allocation factors ≤ 0 or NaN** — The pipeline resets bad factors to 1.0 and logs a warning. Check `output/allocation_issues.csv` for affected processes.

---

## Git History

| Commit | Date | Description |
|--------|------|-------------|
| `668c8e5` | 2025-12-16 | Update LICENSE |
| `e72f6f4` | 2025-12-16 | Working Code for USLCI-Reading to BW2 (initial full commit) |
| `a6556f3` | 2025-12-15 | Initial commit (LICENSE only) |
