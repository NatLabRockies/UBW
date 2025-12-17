import pandas as pd
from pathlib import Path
import brightway2 as bw


"""
Collect unique unit strings from:
  - activity metadata (act['unit'])
  - all exchanges (exc['unit'])
and write a single-column CSV: 'unit'
"""
#bw.projects.set_current("bw2uslci_generator_final")
bw.projects.set_current("uslci_transfer_activities")
#db = bw.Database("uslci_database596")
db = bw.Database('N-SCITE')

units = set()

for act in db:
    u = act['unit']
    if u:
        units.add(str(u).strip())

    # exchanges: production, technosphere, biosphere, etc.
    for exc in act.exchanges():
        u = exc.unit
        if u:
            units.add(str(u).strip())
        else:
            print(exc)

df = pd.DataFrame(sorted(units), columns=["unit"])
df.to_csv("uslci_old_units.csv", index=False)
