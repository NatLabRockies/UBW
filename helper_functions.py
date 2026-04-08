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
# ---------------- ISSUE LOGGING (non-OOP version) -----------------


OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TECH_ISSUES = []
BIO_ISSUES = []
TECH_ISSUE_CSV = OUTPUT_DIR / "technosphere_issues.csv"
BIO_ISSUE_CSV = OUTPUT_DIR / "biosphere_issues.csv"


def _flush_to_csv(path: Path, records: list[dict]) -> None:
    """Append collected records to a CSV file."""
    if not records:
        return
    fieldnames = sorted(set().union(*records))
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(records)
    records.clear()


def log_issue_tech(n_row, info, reason, extra=None):
    """Record an issue related to technosphere exchanges."""
    rec = {
        "reason": reason,
        "process_name": n_row.get("process_name"),
        "exchange_flow_name": n_row.get("exchange_flow_name"),
        "exchange_supplying_process_id": n_row.get("exchange_supplying_process_id"),
        "exchange_supplying_process_name": n_row.get("exchange_supplying_process_name"),
        "exchange_supplying_process_location": n_row.get("exchange_supplying_process_location"),
        "n_row_unit": n_row.get("exchange_unit"),
        "info_unit": info.get("exchange_unit") if isinstance(info, dict) else None,
        "n_row_amount": n_row.get("exchange_amount"),
        "info_process_name": info.get("process_name") if isinstance(info, dict) else None,
        "info_process_location": info.get("process_location") if isinstance(info, dict) else None,
        "info_process_id": info.get("process_id") if isinstance(info, dict) else None,
        "domain": "technosphere",
    }
    if extra:
        rec.update(extra)
    TECH_ISSUES.append(rec)


def log_issue_bio(n_row, info, reason, extra=None):
    """Record an issue related to biosphere exchanges."""
    rec = {
        "reason": reason,
        "process_name": n_row.get("process_name"),
        "exchange_flow_name": n_row.get("exchange_flow_name"),
        "exchange_supplying_process_id": n_row.get("exchange_supplying_process_id"),
        "exchange_supplying_process_name": n_row.get("exchange_supplying_process_name"),
        "exchange_supplying_process_location": n_row.get("exchange_supplying_process_location"),
        "n_row_unit": n_row.get("exchange_unit"),
        "info_unit": info.get("exchange_unit") if isinstance(info, dict) else None,
        "n_row_amount": n_row.get("exchange_amount"),
        "info_process_name": info.get("process_name") if isinstance(info, dict) else None,
        "info_process_location": info.get("process_location") if isinstance(info, dict) else None,
        "info_process_id": info.get("process_id") if isinstance(info, dict) else None,
        "domain": "biosphere",
    }
    if extra:
        rec.update(extra)
    BIO_ISSUES.append(rec)


def flush_technosphere_issues():
    _flush_to_csv(TECH_ISSUE_CSV, TECH_ISSUES)


def flush_biosphere_issues():
    _flush_to_csv(BIO_ISSUE_CSV, BIO_ISSUES)



# --- Unit conversion tables (unchanged logic) ----------------------------------
# Flow-specific conversion factors
# ===============================
# 1) Your base conversions table (kept intact)
# ===============================

flow_conversions = {
    # Natural gas
    "Natural gas, production mixture, to material use": {
        ("kg", "m3"): 1.357,              # 1 kg → 1.357 m³
        ("m3", "kg"): 1 / 1.357,          # 1 m³ → 0.737 kg
        ("kg", "l"): 1.357 * 1000,        # 1 kg → 1357 liters
        ("l", "kg"): 1 / (1.357 * 1000),  # 1 liter → 0.000737 kg
        ("m3", "l"): 1000.0,              # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,               # 1 liter → 0.001 m³
        ("kg", "kg"): 1.0,
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    # Petroleum refined
    "Petroleum refined, to material use, at refinery": {
        ("kg", "m3"): 0.001351351,             # 1 kg → 0.001351351 m³
        ("m3", "kg"): 1 / 0.001351351,         # 1 m³ → ~740 kg
        ("kg", "l"): 0.001351351 * 1000,       # 1 kg → 1.351 liters
        ("l", "kg"): 1 / (0.001351351 * 1000), # 1 liter → 0.74 kg
        ("m3", "l"): 1000.0,                   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,                    # 1 liter → 0.001 m³
        ("kg", "kg"): 1.0,
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    # Transport process
    "Transport, single unit truck, long-haul, diesel powered, Northwest": {
        ("t*km", "kg*km"): 1000.0,  # 1 t·km → 1000 kg·km
        ("kg*km", "t*km"): 0.001,   # 1 kg·km → 0.001 t·km
        ("t*km", "t*km"): 1.0,
        ("kg*km", "kg*km"): 1.0,
    },

    # Transport - generic
    "Transport, single unit truck, long-haul, diesel powered": {
        ("t*km", "kg*km"): 1000.0,
        ("kg*km", "t*km"): 0.001,
        ("t*km", "t*km"): 1.0,
        ("kg*km", "kg*km"): 1.0,
    },

    # LPG
    "Liquefied petroleum gas, at refinery": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 l
        ("l", "m3"): 0.001,    # 1 l → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
        ("t", "l"): 1000.0 / 0.54,   # ≈ 1852 liters per tonne
        ("l", "t"): 0.54 / 1000.0,   # ≈ 0.00054 tonnes per liter
        ("t", "t"): 1.0,
        ("l", "l"): 1.0,
    },
    "Liquefied petroleum gas, combusted in industrial boiler": {
        ("m3", "l"): 1000.0,
        ("l", "m3"): 0.001,
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    "Diesel, at refinery": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 l
        ("l", "m3"): 0.001,    # 1 l → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
        ("t", "l"): 1000.0 / 0.832,   # ≈ 1201 liters per tonne
        ("l", "t"): 0.832 / 1000.0,   # ≈ 0.000832 tonnes per liter
        ("t", "t"): 1.0,
        ("l", "l"): 1.0,
    },

    "Residual fuel oil, at refinery": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 l
        ("l", "m3"): 0.001,    # 1 l → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    "Natural gas, production mixture, to energy use": {
        ("kg", "m3"): 1.357,       # 1 kg → 1.357 m³
        ("m3", "kg"): 1 / 1.357,   # 1 m³ → 0.737 kg
        ("kg", "kg"): 1.0,
        ("m3", "m3"): 1.0,
        ("t", "kg"): 1000.0,   # 1 tonne → 1000 kilograms
        ("kg", "t"): 0.001,    # 1 kilogram → 0.001 tonnes
        ("t", "t"): 1.0,       # identity
        ("kg", "kg"): 1.0,     # identity
    },

    "Electricity, nuclear, at power plant": {
        ("MJ", "kWh"): 1 / 3.6,   # 1 MJ → 0.2778 kWh
        ("kWh", "MJ"): 3.6,       # 1 kWh → 3.6 MJ
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
    },
    "Electricity, bituminous coal, at power plant": {
        ("MJ", "kWh"): 1 / 3.6,   # 1 MJ → 0.2778 kWh
        ("kWh", "MJ"): 3.6,       # 1 kWh → 3.6 MJ
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
    },
    "Electricity, residual fuel oil, at power plant": {
        ("MJ", "kWh"): 1 / 3.6,   # 1 MJ → 0.2778 kWh
        ("kWh", "MJ"): 3.6,       # 1 kWh → 3.6 MJ
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
    },
    "Electricity, biomass, at power plant": {
        ("MJ", "kWh"): 1 / 3.6,   # 1 MJ → 0.2778 kWh
        ("kWh", "MJ"): 3.6,       # 1 kWh → 3.6 MJ
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
    },
    # Electricity from natural gas turbine (cogeneration)
    "Electricity, at cogen, for natural gas turbine": {
        ("MJ", "kWh"): 1 / 3.6,   # 1 MJ → 0.2778 kWh
        ("kWh", "MJ"): 3.6,       # 1 kWh → 3.6 MJ
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
    },
    "Electricity, at grid": {
        ("MJ", "kWh"): 1 / 3.6,     # 1 MJ → 0.2778 kWh
        ("kWh", "MJ"): 3.6,         # 1 kWh → 3.6 MJ
        ("MWh", "kWh"): 1000.0,     # 1 MWh → 1000 kWh
        ("kWh", "MWh"): 0.001,      # 1 kWh → 0.001 MWh
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
        ("MWh", "MWh"): 1.0,
        ("kJ", "kWh"): 1 / 3600.0,  # 1 kJ → 0.0002778 kWh
    },

    "Transport, pipeline, natural gas": {
        ("t*mi", "t*km"): 1.60934,       # 1 ton·mile → 1.60934 ton·km
        ("t*km", "t*mi"): 1 / 1.60934,   # 1 ton·km → 0.621371 ton·mile
        ("t*mi", "t*mi"): 1.0,
        ("t*km", "t*km"): 1.0,
    },

    "Diesel, combusted in industrial equipment": {
        ("m3", "l"): 1000.0,  # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,   # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },
    "Residual fuel oil, combusted in industrial boiler": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,    # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },
    "Gasoline, combusted in industrial boiler": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,    # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },
    "Gasoline, combusted in equipment": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,    # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },
    "Diesel, combusted in industrial boiler": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,    # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    "Gasoline, at refinery": {
        # mass ↔ volume (using density ~0.74 kg/L)
        ("kg", "l"): 1.351,   # 1 kg → ~1.351 liters
        ("l", "kg"): 0.74,    # 1 liter → 0.74 kg
        ("kg", "kg"): 1.0,
        ("l", "l"): 1.0,
        ("t", "l"): 1000.0 / 0.74,   # ≈ 1351 liters per tonne
        ("l", "t"): 0.74 / 1000.0,   # ≈ 0.00074 tonnes per liter
        ("t", "t"): 1.0,
        ("l", "l"): 1.0,

        # volume ↔ volume
        ("m3", "l"): 1000.0,  # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,   # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
    },

    "Gasoline, combusted in equipment, at pulp and paper mill (EXCL.)": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,    # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    "Electricity, AC, 120 V": {
        ("kWh", "MJ"): 3.6,    # 1 kWh = 3.6 MJ
        ("MJ", "kWh"): 1/3.6,  # 1 MJ = 0.2778 kWh
        ("kWh", "kWh"): 1.0,
        ("MJ", "MJ"): 1.0,
        ("btu", "MJ"): 0.001055,      # 1 Btu → 0.001055 MJ
        ("MJ", "btu"): 947.817,       # 1 MJ → 947.817 Btu
        ("btu", "btu"): 1.0,
        ("MJ", "MJ"): 1.0,
    },

    "Transport, combination truck, average fuel mix": {
        ("kg*km", "t*km"): 0.001,   # 1 kg·km → 0.001 t·km
        ("t*km", "kg*km"): 1000.0,  # 1 t·km → 1000 kg·km
        ("kg*km", "kg*km"): 1.0,    # identity
        ("t*km", "t*km"): 1.0,      # identity
    },

    "Natural gas, combusted in industrial boiler": {
        ("btu", "m3"): 1 / 37000.0,   # ≈ 2.7e-05 m³ per Btu
        ("m3", "btu"): 37000.0,       # ≈ 37,000 Btu per m³
        ("btu", "btu"): 1.0,          # identity
        ("m3", "m3"): 1.0,            # identity
        ("MJ", "m3"): 1.0 / 37.0,     # ≈ 0.027 m³ per MJ
        ("m3", "MJ"): 37.0,           # ≈ 37 MJ per m³
        ("MJ", "MJ"): 1.0,
        ("m3", "m3"): 1.0,
    },

    "Kerosene, at refinery": {
        ("m3", "l"): 1000.0,   # 1 m³ → 1000 liters
        ("l", "m3"): 0.001,    # 1 liter → 0.001 m³
        ("m3", "m3"): 1.0,     # identity
        ("l", "l"): 1.0,       # identity
    },

    "Electricity, natural gas, at power plant": {
        ("MJ", "kWh"): 1.0 / 3.6,   # ≈ 0.27778 kWh per MJ
        ("kWh", "MJ"): 3.6,         # 1 kWh = 3.6 MJ
        ("MJ", "MJ"): 1.0,
        ("kWh", "kWh"): 1.0,
    },

    "Compressed natural gas, dispensed at pump": {
        ("btu", "t"): 1.0 / 50_800_000.0,  # ≈ 1.97e-8 tonnes per Btu
        ("t", "btu"): 50_800_000.0,        # ≈ 50.8 million Btu per tonne
        ("btu", "btu"): 1.0,
        ("t", "t"): 1.0,
    },
    "Gasoline, dispensed at pump": {
        ("btu", "t"): 1.0 / 44_000_000.0,  # ≈ 2.27e-8 tonnes per Btu
        ("t", "btu"): 44_000_000.0,        # ≈ 44 million Btu per tonne
        ("btu", "btu"): 1.0,
        ("t", "t"): 1.0,
    },
    "Diesel, dispensed at pump": {
        ("btu", "t"): 1.0 / 43_100_000.0,  # ≈ 2.32e-8 tonnes per Btu (HHV ~45.5 MJ/kg)
        ("t", "btu"): 43_100_000.0,        # ≈ 43.1 million Btu per tonne
        ("btu", "btu"): 1.0,
        ("t", "t"): 1.0,
    },

    "Drinking water treatment; U.S. Average Treatment": {
        ("m3", "l"): 1000.0,           # 1 cubic meter = 1000 liters
        ("l", "m3"): 1.0 / 1000.0,     # 1 liter = 0.001 cubic meter
        ("m3", "m3"): 1.0,
        ("l", "l"): 1.0,
    },

    "Construction sand and gravel; at mine": {
        ("kg", "lb_av"): 2.20462262185,
        ("lb_av", "kg"): 1.0 / 2.20462262185,
        ("kg", "kg"): 1.0,
        ("lb_av", "lb_av"): 1.0,
    },
}

# ===============================
# 2) Merge-in everything from your error list + missing energy links
# ===============================

def _merge_flow(flow_name, pairs):
    if flow_name not in flow_conversions:
        flow_conversions[flow_name] = {}
    flow_conversions[flow_name].update(pairs)

_LBAV_PER_KG = 2.20462262185
_KG_PER_LBAV = 0.45359237
_L_PER_GAL = 3.785411784
_GAL_PER_L = 1.0 / _L_PER_GAL
_M3_PER_FT3 = 0.028316846592
_FT3_PER_M3 = 1.0 / _M3_PER_FT3
_FT_PER_M = 3.280839895013123
_M_PER_FT = 0.3048
_KG_PER_G = 0.001
_G_PER_KG = 1000.0
_KM_PER_MI = 1.60934
_MI_PER_KM = 1.0 / _KM_PER_MI

def _lb_pairs():
    return {
        ("kg", "lb_av"): _LBAV_PER_KG,
        ("lb_av", "kg"): _KG_PER_LBAV,
        ("kg", "lb av"): _LBAV_PER_KG,
        ("lb av", "kg"): _KG_PER_LBAV,
        ("lb_av", "lb_av"): 1.0,
        ("lb av", "lb av"): 1.0,
        ("kg", "kg"): 1.0,
    }

for _flow in [
    "Construction sand and gravel; at mine",
    "Bituminous coal, combusted in industrial boiler",
    "Portland cement, at plant",
    "Sodium hydroxide; at plant",
    "Sodium tripolyphosphate, at plant",
    "Aluminium sulphate, powder, at plant",
    "Polymers, at plant",
    "Hydrogen fluoride, at plant",
    "Quicklime, at plant",
    "Chlorine; chlor-alkali electrolysis; at plant",
    "Silica sand, at plant",
    "Steel scrap recovery",
    "Cement mortar for ductile iron pipe",
    "Oxygen, liquid, at plant",
    "Metallurgical coke, at plant",
    "Granular activated carbon; at plant",
    "Drinking water treatment; Granular activated carbon regeneration",
    "Sodium hypochlorite, 15% in H2O, at plant",
    "Iron sulphate, at plant",
]:
    _merge_flow(_flow, _lb_pairs())

for _flow in [
    "Drinking water treatment; Adsorption infrastructure",
    "Drinking water treatment; Chemical Conditioning infrastructure",
    "Drinking water treatment; Distribution Pipe Network Infrastructure",
    "Drinking water treatment; Valves for distribution system",
    "Drinking water treatment; Pumps for distribution system",
    "Drinking water treatment; Motors for distribution system",
    "Drinking water treatment; Water storage infrastructure",
    "Drinking water treatment; Flocculation infrastructure",
    "Drinking water treatment; Fluoridation infrastructure",
    "Drinking water treatment; Lime addition infrastructure",
    "Drinking water treatment; Primary chlorine disinfection infrastructure",
    "Drinking water treatment; Sand filtration infrastructure",
    "Drinking water treatment; Sedimentation infrastructure",
    "Drinking water treatment; Source water acquisition infrastructure",
    "Drinking water treatment; Primary Disinfection, Conventional UV",
    "Drinking water treatment; Pre-Disinfection Processes, no Adsorption",
]:
    _merge_flow(_flow, {
        ("cu ft", "m3"): _M3_PER_FT3,
        ("m3", "cu ft"): _FT3_PER_M3,
        ("cu ft", "cu ft"): 1.0,
        ("m3", "m3"): 1.0,
    })

_merge_flow("Drinking water; with chlorine disinfection; at consumer", {
    ("l", "cu ft"): _FT3_PER_M3 * 0.001,
    ("cu ft", "l"): 1000.0 * _M3_PER_FT3,
    ("l", "l"): 1.0,
    ("cu ft", "cu ft"): 1.0,
})

for _flow in [
    "Gasoline, combusted in equipment",
    "Diesel, combusted in industrial boiler",
    "Diesel, combusted in industrial equipment",
    "Liquefied petroleum gas, combusted in industrial boiler",
    "Residual fuel oil, combusted in industrial boiler",
]:
    _merge_flow(_flow, {
        ("gal (US liq)", "l"): _L_PER_GAL,
        ("l", "gal (US liq)"): _GAL_PER_L,
        ("gal (US liq)", "gal (US liq)"): 1.0,
        ("l", "l"): 1.0,
    })

_merge_flow("Natural gas, combusted in industrial boiler", {
    ("cu ft", "m3"): _M3_PER_FT3,
    ("m3", "cu ft"): _FT3_PER_M3,
})

for _flow in [
    "Transport, combination truck, diesel powered",
    "Transport, barge, average fuel mix",
    "Transport, train, diesel powered",
]:
    _merge_flow(_flow, {
        ("t*mi", "t*km"): _KM_PER_MI,
        ("t*km", "t*mi"): _MI_PER_KM,
        ("t*mi", "t*mi"): 1.0,
        ("t*km", "t*km"): 1.0,
    })

for _flow in [
    'Ductile Iron Pipe, 10"', 'Ductile Iron Pipe, 12"', 'Ductile Iron Pipe, 16"', 'Ductile Iron Pipe, 20"',
    'Ductile Iron Pipe, 24"', 'Ductile Iron Pipe, 35"', 'Ductile Iron Pipe, 36"', 'Ductile Iron Pipe, 44"',
    'Ductile Iron Pipe, 50"', 'Ductile Iron Pipe, 54"', 'Ductile Iron Pipe, 60"', 'Ductile Iron Pipe, 72"',
    'Concrete Pipe, 6"', 'Concrete Pipe, 8"', 'Concrete Pipe, 12"', 'Concrete Pipe, 36"',
    'Concrete Pipe, 50"', 'Concrete Pipe, 54"', 'Concrete Pipe, 60"', 'Concrete Pipe, 72"',
    'Concrete Pipe, 78"', 'Concrete Pipe, 84"','Ductile Iron Pipe, 6"','Ductile Iron Pipe, 8"'
]:
    _merge_flow(_flow, {
        ("m", "ft"): _FT_PER_M,
        ("ft", "m"): _M_PER_FT,
        ("m", "m"): 1.0,
        ("ft", "ft"): 1.0,
    })

for _flow in [
    "Argon, liquid, at plant",
    "molybdenum, at regional storage",
    "Flat glass, uncoated, at plant",
    "indium, at regional storage",
    "Ceramic tiles, at regional storage",
    "mercury, liquid, at plant",
    "Ethanol, 85%, at blending terminal",
    "Sodium bicarbonate; at plant",
    "Carbon monoxide, at plant",
    "Sodium hydroxide; at plant",
]:
    _merge_flow(_flow, {
        ("g", "kg"): _KG_PER_G,
        ("kg", "g"): _G_PER_KG,
        ("g", "g"): 1.0,
        ("kg", "kg"): 1.0,
    })

_MJ_per_kWh = 3.6
_kWh_per_MJ = 1.0 / _MJ_per_kWh
_kJ_per_kWh  = 3600.0
_kWh_per_kJ  = 1.0 / _kJ_per_kWh
_kWh_per_MWh = 1000.0
_MWh_per_kWh = 1.0 / _kWh_per_MWh
_Btu_per_MJ  = 947.817
_Btu_per_kWh = _Btu_per_MJ * _MJ_per_kWh
_kWh_per_Btu = 1.0 / _Btu_per_kWh

_ELECTRICITY_EXTRA = {
    ("kJ", "kWh"): _kWh_per_kJ,
    ("kWh", "kJ"): _kJ_per_kWh,
    ("MWh", "kWh"): _kWh_per_MWh,
    ("kWh", "MWh"): _MWh_per_kWh,
    ("MWh", "MJ"):  _kWh_per_MWh * _MJ_per_kWh,
    ("MJ", "MWh"):  1.0 / (_kWh_per_MWh * _MJ_per_kWh),
    ("MWh", "kJ"):  _kWh_per_MWh * _kJ_per_kWh,
    ("kJ", "MWh"):  1.0 / (_kWh_per_MWh * _kJ_per_kWh),
    ("kWh", "btu"): _Btu_per_kWh,
    ("btu", "kWh"): _kWh_per_Btu,
    ("kJ", "kJ"): 1.0,
    ("MWh", "MWh"): 1.0,
    ("kWh", "kWh"): 1.0,
    ("MJ", "MJ"): 1.0,
    ("btu", "btu"): 1.0,
}
for _flow in [
    "Electricity, nuclear, at power plant",
    "Electricity, bituminous coal, at power plant",
    "Electricity, residual fuel oil, at power plant",
    "Electricity, biomass, at power plant",
    "Electricity, at cogen, for natural gas turbine",
    "Electricity, at grid",
    "Electricity, natural gas, at power plant",
    "Electricity, AC, 120 V",
]:
    _merge_flow(_flow, _ELECTRICITY_EXTRA)
_merge_flow("Electricity, at grid", { ("kWh", "kJ"): _kJ_per_kWh })

_HHV_GASOLINE = 46.4
_HHV_DIESEL   = 45.5
_HHV_LPG      = 46.1
_HHV_NG_MASS  = 55.0
_RHO_GASOLINE = 0.74
_RHO_DIESEL   = 0.832
_RHO_LPG      = 0.54

def _add_energy_links(flow, HHV, rho=None):
    pairs = {
        ("MJ", "kg"): 1.0 / HHV,
        ("kg", "MJ"): HHV,
    }
    if rho is not None:
        MJ_per_L = HHV * rho
        pairs.update({
            ("MJ", "l"): 1.0 / MJ_per_L,
            ("l", "MJ"): MJ_per_L,
            ("l", "l"): 1.0,
            ("MJ", "MJ"): 1.0,
        })
    _merge_flow(flow, pairs)

_add_energy_links("Gasoline, at refinery", _HHV_GASOLINE, _RHO_GASOLINE)
_add_energy_links("Diesel, at refinery", _HHV_DIESEL, _RHO_DIESEL)
_add_energy_links("Liquefied petroleum gas, at refinery", _HHV_LPG, _RHO_LPG)
_add_energy_links("Natural gas, production mixture, at processing", _HHV_NG_MASS)


def convert_amount_exchange_to_supplier(amount: float, exchange_unit: str, supplier_unit: str):
    """
    Convert an amount from exchange_unit to supplier_unit.

    Supported conversions:
      - m3 → L     (× 1000)
      - MJ → kWh   (÷ 3.6)
    If no match, returns amount unchanged.

    Returns (converted_amount, supplier_unit)
    """
    if amount is None:
        return amount, supplier_unit

    e = str(exchange_unit).strip().lower()
    s = str(supplier_unit).strip().lower()

    # Volume conversion
    if e == "m3" and s in {"l", "liter", "litre"}:
        return amount * 1000.0, supplier_unit

    # Energy conversion
    elif e == "mj" and s == "kwh":
        return amount / 3.6, supplier_unit

    # Otherwise no change
    return amount, exchange_unit


def load_unit_name_bridge(csv_path: str) -> dict:
    """
    Load a CSV with columns ['unit', 'unit_new'] into a mapping dictionary.
    Ignores blanks or NaNs.
    """
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["unit", "unit_new"])
    df["unit"] = df["unit"].astype(str).str.strip().str.lower()
    df["unit_new"] = df["unit_new"].astype(str).str.strip()
    bridge = dict(zip(df["unit"], df["unit_new"]))
    print(f"✅ Loaded {len(bridge)} unit name mappings from {csv_path}")
    return bridge


def convert_unit_name(unit: str, bridge: dict) -> str:
    """
    Convert a unit name to its standardized form using the provided bridge dict.
    If no match found, returns the original unit unchanged.
    """
    if not unit:
        return unit
    unit_norm = str(unit).strip().lower()
    return bridge.get(unit_norm, unit)


def convert_amount(value, from_unit, to_unit, flow_name):
    """Convert value from from_unit to to_unit, only if flow is recognized."""
    if flow_name not in flow_conversions:
        print(f"No conversion rules defined for flow: {flow_name}: {from_unit} → {to_unit}")
        return 1
    try:
        return value * flow_conversions[flow_name][(from_unit, to_unit)]
    except KeyError:
        print(f"No conversion available for {flow_name}: {from_unit} → {to_unit}")
        return 1

def clear_technosphere_and_biosphere_exchanges(act):
    """Delete all technosphere & biosphere exchanges on an activity; keep production."""
    removed = 0
    for exc in list(act.technosphere()):
        exc.delete()
        removed += 1
    for exc in list(act.biosphere()):
        exc.delete()
        removed += 1
    act.save()
    return removed


def debugger(lca,mydata_db):
    # --- Diagnostics ---------------------------------------------------------------
    x = lca.supply_array
    neg_idx = np.where(x < 0)[0]
    print("Negative activity levels:", len(neg_idx))
    
    rev_act = {v: k for k, v in lca.activity_dict.items()}
    for i in neg_idx[:10]:
        act_key = rev_act[i]
        act = bw.get_activity(act_key)
        print(f"x={x[i]:.6g} | {act.get('name')} [{act.get('location')}] in DB '{act.key[0]}'")
    
    A = lca.technosphere_matrix.tocsc()
    diag = A.diagonal()
    print("Technosphere diag min/max:", diag.min(), diag.max())
    print("Columns with diag >= 0:", int((diag >= 0).sum()))
    
    lca.lcia()
    C = lca.characterized_inventory.tocoo()
    rev_bio = {v: k for k, v in lca.biosphere_dict.items()}
    neg_terms = []
    for r, c, v in zip(C.row, C.col, C.data):
        if v < 0:
            flow_key = rev_bio[r]
            act_key  = rev_act[c]
            flow = bw.get_activity(flow_key)
            act  = bw.get_activity(act_key)
            neg_terms.append((v, flow.get('name'), flow.get('unit'), act.get('name')))
    
    print("Sample negative characterized terms:")
    for v, fname, funit, aname in sorted(neg_terms)[:10]:
        print(f"{v:.6g}  |  {fname} [{funit}]  in  {aname}")
    
    A = lca.technosphere_matrix.tocoo()
    pos_offdiag = []
    for r,c,v in zip(A.row, A.col, A.data):
        if r != c and v > 0:
            pos_offdiag.append((r,c,v))
    print("Positive off-diagonals (should be zero):", len(pos_offdiag))
    
    A = lca.technosphere_matrix.tocsc()
    diag = A.diagonal()
    
    idx_max = int(np.argmax(diag))
    val_max = float(diag[idx_max])
    
    rev_act = {v: k for k, v in lca.activity_dict.items()}
    act_key = rev_act[idx_max]
    act = bw.get_activity(act_key)
    
    print("\n=== Largest diagonal ===")
    print(f"index: {idx_max}")
    print(f"diag value (production amount in matrix units): {val_max:g}")
    print(f"activity: {act.get('name')} [{act.get('location')}] in DB '{act.key[0]}'")
    print(f"reference product: {act.get('reference product')}")
    print(f"activity 'production amount' metadata: {act.get('production amount')}  | unit: {act.get('unit')}")
    
    prods = [e for e in act.exchanges() if e['type'] == 'production']
    print(f"num production exchanges recorded: {len(prods)}")
    if prods:
        print(f"production exchange amount: {prods[0]['amount']}  | input==act.key? {prods[0].input == act.key}")
    
    col = A.getcol(idx_max).tocoo()
    rows, data = col.row, col.data
    
    off_mask = rows != idx_max
    off_rows = rows[off_mask]
    off_vals = data[off_mask]
    
    n_pos_off = int((off_vals > 0).sum())
    n_neg_off = int((off_vals < 0).sum())
    sum_off   = float(off_vals.sum())
    
    print("\n=== Column diagnostics ===")
    print(f"off-diagonals: total={off_vals.size} | negatives(inputs)={n_neg_off} | positives(suspicious)={n_pos_off}")
    print(f"sum(off-diagonals)={sum_off:g} | diag={val_max:g}")
    
    top = 10
    mag_idx = np.argsort(np.abs(off_vals))[::-1][:top]
    print("\nTop off-diagonals by |value|:")
    for i in mag_idx:
        r = int(off_rows[i]); v = float(off_vals[i])
        prov_key = rev_act[r]
        prov = bw.get_activity(prov_key)
        print(f"{v: .6g}  from  {prov.get('name')} [{prov.get('location')}] in DB '{prov.key[0]}'")
    
    # --- Extract CFs (save → output/) ---------------------------------------------
    recipe_methods = [list(bw.methods)[639]]
    print(f"Found {len(recipe_methods)} ReCiPe methods")
    
    records = []
    for method in recipe_methods:
        method_obj = bw.Method(method)
        cf_data = method_obj.load()
        for flow, cf in cf_data:
            try:
                db_name, flow_key = flow
                flow_obj = mydata_db.get(flow_key)
                flow_name = flow_obj['name']
                flow_unit = flow_obj['unit']
                flow_categories = flow_obj.get('categories', None)
            except:
                flow_name = str(flow)
                flow_unit = None
                flow_categories = None
            records.append({
                "method": str(method),
                "flow_name": flow_name,
                "flow_unit": flow_unit,
                "flow_categories": flow_categories,
                "cf": cf
            })
    
    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_DIR / "ReCiPe_characterization_factors.csv", index=False, encoding="utf-8")
    print("✅ Saved all ReCiPe CFs to output/ReCiPe_characterization_factors.csv")


def elem_flows_extractor(test_data):  
    # --- Elementary flows list & biosphere3 export (→ output/) --------------------
    # Filter only elementary flows
    elementary_flows = test_data[test_data['exchange_type'] == "ELEMENTARY_FLOW"]
    elementary_flows_out = elementary_flows[
        [
            "exchange_flow_name",
            "exchange_flow_id",
            "exchange_unit",
            "exchange_type",
            "exchange_category",
            "exchange_description"
        ]
    ]
    elementary_flows_out = elementary_flows_out.replace("Error:This flow is missing description", "", regex=False)
    elementary_flows_out['text'] =  elementary_flows_out['exchange_flow_name'] + "."+ elementary_flows_out["exchange_category"] + "."+ elementary_flows_out["exchange_description"] + ".\n"
    elementary_flows_out = elementary_flows_out.drop_duplicates()
    elementary_flows_out.to_csv(OUTPUT_DIR / "elementary_flows.csv", index=False)
    print(f"Saved {len(elementary_flows_out)} elementary flows to 'output/elementary_flows.csv'")
    
    # Load biosphere3
    biosphere = bw.Database("biosphere3")
    rows = []
    for flow in biosphere:
        data = flow.as_dict()
        rows.append({
            "key": str(flow.key),
            "code": flow.get("code"),
            "name": flow.get("name"),
            "unit": flow.get("unit"),
            "categories": "; ".join(flow.get("categories", [])) if flow.get("categories") else None,
            "type": flow.get("type"),
            "id": flow.key[1],
            "database": flow.key[0],
            "location": flow.get("location"),
            "reference product": flow.get("reference product"),
            "comment": flow.get("comment"),
        })
    biosphere_df = pd.DataFrame(rows)
    biosphere_df.to_csv(OUTPUT_DIR / "biosphere3_export.csv", index=False, encoding="utf-8")
    print(f"Exported {len(biosphere_df)} flows from biosphere3 → output/biosphere3_export.csv")