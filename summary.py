import os
import csv
import argparse
import statistics

def collate_summary(input_dir, output_summary_file):
    """Collates CATH summary files"""
    cath_summary = {"UNASSIGNED": 0}  # Initialize UNASSIGNED explicitly

    parsed_files = [f for f in os.listdir(input_dir) if f.endswith(".parsed")]

    for file in parsed_files:
        file_path = os.path.join(input_dir, file)
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
                if len(lines) < 2:
                    print(f"Skipping empty or malformed file: {file_path}")
                    continue

                # Process cath_id and count from remaining lines
                for line in lines[2:]:
                    cath_id, count = line.strip().split(",")
                    count = int(count)
                    
                    # Handle UNASSIGNED explicitly
                    if cath_id == "UNASSIGNED":
                        cath_summary["UNASSIGNED"] += count
                    else:
                        if cath_id not in cath_summary:
                            cath_summary[cath_id] = 0
                        cath_summary[cath_id] += count

        except Exception as e:
            print(f"Error processing file {file}: {e}")

    # Write summary to CSV
    with open(output_summary_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["cath_id", "count"])
        for cath_id, count in cath_summary.items():
            csv_writer.writerow([cath_id, count])

    print(f"Summary saved to {output_summary_file}")


def calculate_mean_and_std_plddt(input_dir, output_mean_file, dataset_name):
    """Calculates mean and standard deviation of plDDT scores."""
    parsed_files = [f for f in os.listdir(input_dir) if f.endswith(".parsed")]

    plddt_scores = []

    for file in parsed_files:
        file_path = os.path.join(input_dir, file)
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
                if len(lines) < 2:
                    print(f"Skipping empty or malformed file: {file_path}")
                    continue

                # Extract mean_plddt from the first line
                first_line = lines[0].strip()
                mean_plddt_part = float(first_line.split(":")[-1].strip())

                plddt_scores.append(mean_plddt_part)

        except Exception as e:
            print(f"Error processing file {file}: {e}")

    mean_plddt = statistics.mean(plddt_scores) if plddt_scores else 0
    std_plddt = statistics.stdev(plddt_scores) if len(plddt_scores) > 1 else 0

    # Append mean and std plDDT to the output file
    with open(output_mean_file, "a", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([dataset_name, mean_plddt, std_plddt])

    print(f"Mean and Std plDDT for {dataset_name} saved to {output_mean_file}")


def main():
    human_input_dir = "/mnt/minio/dataset/human/output"
    ecoli_input_dir = "/mnt/minio/dataset/ecoli/output"

    human_summary_file = "human_cath_summary.csv"
    ecoli_summary_file = "ecoli_cath_summary.csv"
    plddt_mean_file = "plDDT_means.csv"

    # Initialize the plDDT mean file with headers
    with open(plddt_mean_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["organism", "mean plddt", "plddt std"])

    # Collate summaries
    collate_summary(human_input_dir, human_summary_file)
    collate_summary(ecoli_input_dir, ecoli_summary_file)

    # Calculate mean and std plDDT
    calculate_mean_and_std_plddt(human_input_dir, plddt_mean_file, "human")
    calculate_mean_and_std_plddt(ecoli_input_dir, plddt_mean_file, "ecoli")


if __name__ == "__main__":
    main()
