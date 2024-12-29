import os
import sys
import csv
import json
from collections import defaultdict
import statistics

# Input arguments
output_dir = sys.argv[1]  # Output directory
input_file = sys.argv[2]  # Full path to the .tsv file
id = os.path.basename(input_file).rstrip("_search.tsv")
parsed_file_path = os.path.join(output_dir, f"{id}.parsed")

print(f"Processing file: {input_file}")

if not os.path.exists(input_file):
    print(f"ERROR: File {input_file} not found.")
    sys.exit(1)

cath_ids = defaultdict(int)
plDDT_values = []

try:
    with open(input_file, "r") as fhIn:
        next(fhIn)
        reader = csv.reader(fhIn, delimiter="\t")
        for row in reader:
            plDDT_values.append(float(row[3]))
            meta = json.loads(row[15])
            cath_ids[meta["cath"]] += 1
except Exception as e:
    print(f"Error reading or processing {input_file}: {e}")
    sys.exit(1)

try:
    with open(parsed_file_path, "w", encoding="utf-8") as fhOut:
        mean_plDDT = statistics.mean(plDDT_values) if plDDT_values else 0
        fhOut.write(f"#{id} Results. mean plddt: {mean_plDDT}\n")
        fhOut.write("cath_id,count\n")
        for cath_id, count in cath_ids.items():
            fhOut.write(f"{cath_id},{count}\n")
    print(f"Parsed output saved to: {parsed_file_path}")
except Exception as e:
    print(f"Error writing parsed file {parsed_file_path}: {e}")