import os
import csv
import argparse

def collate_parsed_files(input_dir, output_file):
    # List all .parsed files in the directory
    parsed_files = [f for f in os.listdir(input_dir) if f.endswith(".parsed")]
    
    if not parsed_files:
        print("No .parsed files found in the specified directory.")
        return

    # Prepare the output CSV
    with open(output_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        
        # Write the header row
        csv_writer.writerow(["id", "mean_plddt", "cath_id", "count"])
        
        # Process each .parsed file
        for file in parsed_files:
            file_path = os.path.join(input_dir, file)
            try:
                with open(file_path, "r") as f:
                    lines = f.readlines()
                    if len(lines) < 2:
                        print(f"Skipping empty or malformed file: {file_path}")
                        continue
                    
                    # Extract the id and mean_plddt from the first line
                    first_line = lines[0].strip()
                    id_part = first_line.split()[0].lstrip("#")
                    mean_plddt_part = first_line.split(":")[-1].strip()
                    
                    # Extract cath_id and count from the remaining lines
                    for line in lines[2:]:
                        cath_id, count = line.strip().split(",")
                        # Write the row to the output CSV
                        csv_writer.writerow([id_part, mean_plddt_part, cath_id, count])
            
            except Exception as e:
                print(f"Error processing file {file}: {e}")

    print(f"Collation complete. Output saved to {output_file}")

if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Collate data from .parsed files into a CSV.")
    parser.add_argument("input_dir", type=str, help="Directory containing .parsed files")
    parser.add_argument("output_file", type=str, help="Path to the output CSV file")

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the provided arguments
    collate_parsed_files(args.input_dir, args.output_file)
