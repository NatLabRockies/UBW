# USLCI → Brightway2 Inventory Builder

This repository contains a full pipeline for **converting the USLCI database into a Brightway2-compatible format**, including:

- Cleaning raw USLCI data  
- Handling multiproduct processes via allocation  
- Correcting exchange IDs  
- Building a Brightway2 database programmatically  
- Adding technosphere and biosphere exchanges  
- Running matrix validation and LCIA diagnostics  
- Exporting results and characterization factors  

The workflow enables **reproducible, validated LCA inventories** within the Brightway2 framework.

---

## 🚀 Quick Start

1. **Clone the repository**  
   ```bash
   git clone https://github.com/yourusername/bw2uslci.git
   cd bw2uslci
   ```

2. **Set up environment**  
   ```bash
   conda create -n bw2uslci python=3.11
   conda activate bw2uslci
   pip install brightway2 bw2io numpy pandas
   ```

3. **Configure Brightway2 directory**  
   Edit your script or run before starting:
   ```python
   import os
   os.environ['BRIGHTWAY2_DIR'] = "/path/to/bw2uslci_test/"
   ```

4. **Run the pipeline**
   ```bash
   python backup_plan.py
   ```

5. **Update biosphere flow mappings** (optional but recommended)
   `working_bridge.csv` may not cover all elementary flows in `uslci.csv`. The bridge mapper skill identifies unmapped flows, matches them against biosphere3 by name and compartment, and appends new mappings to `working_bridge.csv`. See [Bridge Mapper](#-bridge-mapper) below.

   After updating the bridge, re-run the pipeline:
   ```bash
   python backup_plan.py
   ```

6. **Check results**
   - `process_product_corrected.csv` → cleaned processes
   - `lcia_results.csv` → LCIA results for all activities
   - `exchange_issues.csv` → logged technosphere/biosphere issues

---

## 📂 Project Structure

```
.
├── uslci.csv                  # Raw USLCI input (after preprocessing, e.g., removing waste flows)
├── working_bridge.csv          # Mapping between USLCI elementary flows and biosphere3 flows
├── allocation_exchange_df.csv  # Allocation factors for multiproduct processes
├── process_product_corrected.csv  # Intermediate: cleaned, corrected processes
├── ReCiPe_characterization_factors.csv # Exported CFs
├── elementary_flows.csv        # Exported USLCI elementary flows
├── biosphere3_export.csv       # Full biosphere3 export
├── lcia_results.csv            # LCIA results for all activities
├── backup_plan.py              # Main Brightway2 pipeline script
└── README.md                   # This file
```

---

## ⚙️ Setup

### 1. Environment
Create a conda environment with Brightway2 and dependencies:

```bash
conda create -n bw2uslci python=3.11
conda activate bw2uslci
pip install brightway2 bw2io numpy pandas
```

Optional (for exports/backups):
```bash
pip install bw2io[export]
```

### 2. Configure Brightway2 directory
By default Brightway2 uses `~/Brightway2/`. This project sets a custom directory:

```python
import os
os.environ['BRIGHTWAY2_DIR'] = "/Users/tghosh/Desktop/bw2uslci_test/"
```

Make sure to set this path consistently.

---

## 🛠️ Workflow

### 1. Project Initialization
```python
import brightway2 as bw
bw.projects.set_current('bw2uslci_generator_test')
bw.bw2setup()
```

### 2. Data Cleaning
- Load `uslci.csv`  
- Replace missing/invalid locations with `"US"`  
- Remove waste flows (`exchange_type == "WASTE_FLOW"`)  
- Ensure one product per process (split multiproducts into separate processes)  
- Correct duplicate process/exchange IDs  

Output: `process_product_corrected.csv`

### 3. Brightway2 Database Creation
- Create new database (e.g. `"testest"`)  
- For each process:
  - Add activity with metadata (`code`, `name`, `unit`, `location`)  
  - Add production exchange (reference product)  

### 4. Add Technosphere Exchanges
- Re-link technosphere flows between activities  
- Handle unit mismatches with **conversion factors** (`flow_conversions` dictionary)  
- Log issues (e.g., missing suppliers, self-loops) in `exchange_issues.csv`

### 5. Add Biosphere Exchanges
- Use `working_bridge.csv` to map USLCI elementary flows to `biosphere3`  
- Add biosphere exchanges to activities  
- Log missing/unmapped flows

### 6. Diagnostics
- Validate locations (NaN-like or missing from `geomapping`)  
- Check technosphere matrix:
  - Diagonal entries strictly negative  
  - Off-diagonal entries non-positive  
  - No self-supply loops  
- Identify negative activity levels and suspicious off-diagonals  

### 7. LCIA
- Run LCIA with selected methods (e.g. ReCiPe, IPCC GWP100)  
- Export all characterization factors for chosen methods to `ReCiPe_characterization_factors.csv`  
- Collect and save results for **all activities** in:
  ```
  lcia_results.csv
  ```

### 8. Exports
- `elementary_flows.csv`: USLCI elementary flows  
- `biosphere3_export.csv`: Full biosphere3 flows for reference  
- (Optional) export activities in **Ecospold1/2 format** using `bw2io.export`  

### 9. Backup
Backup projects for portability:

```python
from bw2io.backup import backup_project_directory
backup_project_directory("testest")
```

---

## 📊 Validation Outputs

Examples from diagnostics:

**Technosphere diagonal sanity**
```
Technosphere diag min/max: -1.0 -0.001
Columns with diag >= 0: 0
```

**Largest diagonal inspection**
```
activity: Electricity, aluminum smelting and ingot casting regions [Northern America]
reference product: Electricity
production amount: 1.0 kWh
```

**LCIA results CSV includes:**
- Activity metadata (name, db, code, product, unit, location)  
- Method details (name, unit, category)  
- LCIA score  

---

## ✅ Key Features
- Handles **multi-output processes** via allocation  
- Includes **unit harmonization** for energy, mass, transport, and other flows  
- Robust **issue logging** for cutoff flows, missing suppliers, unmapped biosphere flows  
- Extensive **sanity checks** on matrices and LCIA results  
- Outputs in **Brightway2, CSV, and Ecospold** formats  
- Reproducible via full **project backups**  

---

## 🌉 Bridge Mapper

`working_bridge.csv` maps USLCI elementary flow IDs to biosphere3 UUIDs. Flows not in the bridge are dropped during step 5 (Add Biosphere Exchanges), leading to incomplete LCIA results.

The [uslci-bridge-mapper](https://github.com/sergiomor/uslci-bridge-mapper) is a Claude Code skill that automates the creation of missing mappings using exact name + compartment matching and AI-assisted fuzzy matching (spelling variants, IUPAC/common names, CFC trade names, oxidation states, etc.).

Tested on USLCI 2025 Q1: improved `working_bridge.csv` from 2,424 to 3,250 mappings (+826), reducing unmapped flows from 1,495 to 669.

### Usage

Requires [Claude Code](https://docs.anthropic.com/en/docs/claude-code).

1. Fork this repository
2. Download the [uslci-bridge-mapper](https://github.com/sergiomor/uslci-bridge-mapper) skill into `.claude/skills/`
3. Run `backup_plan.py` at least once (so biosphere3 exists)
4. Run `/uslci-bridge-mapper` in Claude Code
5. Re-run `backup_plan.py` to rebuild with the updated bridge
6. PR the updated `working_bridge.csv` back to this repo

See the [skill README](https://github.com/sergiomor/uslci-bridge-mapper) for full documentation, requirements, and the list of unfixable flows.

---

## 🔮 Future Work
- Add support for economic or system expansion allocation
- Extend exporter to generate Ecospold2 for openLCA/SimaPro import
- ~~Automate mapping of USLCI → biosphere3 flows using NLP/AI tools~~ (implemented via Bridge Mapper)
