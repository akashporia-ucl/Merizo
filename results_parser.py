import os
import sys
import csv
import json
from collections import defaultdict
import statistics

cath_ids = defaultdict(int)
plDDT_values = []

id = sys.argv[2].rstrip("_search.tsv")

# Use absolute path
input_file = os.path.abspath(os.path.join(sys.argv[1], sys.argv[2]))

# Debugging: Print the absolute path being opened
print(f"ABS PATH: {input_file}")
print(f"Trying to open: {input_file}")

# Check if file exists
if not os.path.exists(input_file):
    print(f"ERROR: File '{input_file}' does not exist.")
    sys.exit(1)

# File processing
with open(input_file, "r") as fhIn:
    next(fhIn)
    msreader = csv.reader(fhIn, delimiter='\t')
    tot_entries = 0
    for i, row in enumerate(msreader):
        tot_entries = i + 1
        plDDT_values.append(float(row[3]))
        meta = row[15]
        data = json.loads(meta)
        cath_ids[data["cath"]] += 1

# Writing output
with open(id + ".parsed", "w", encoding="utf-8") as fhOut:
    if len(plDDT_values) > 0:
        fhOut.write(f"#{sys.argv[2]} Results. mean plddt: {statistics.mean(plDDT_values)}\n")
    else:
        fhOut.write(f"#{sys.argv[2]} Results. mean plddt: 0\n")
    fhOut.write("cath_id,count\n")
    for cath, v in cath_ids.items():
        fhOut.write(f"{cath},{v}\n")
