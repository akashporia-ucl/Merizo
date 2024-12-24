import sys
from subprocess import Popen, PIPE
import glob
import os
import multiprocessing
from Bio.PDB import MMCIFParser, PDBIO

"""
usage: python pipeline_script.py [INPUT DIR] [OUTPUT DIR]
approx 5seconds per analysis
"""

def convert_cif_to_pdb(cif_file, pdb_file):
    """
    Converts a .cif file to a .pdb file using BioPython's MMCIFParser.
    """
    parser = MMCIFParser()
    structure = parser.get_structure("structure", cif_file)
    io = PDBIO()
    io.set_structure(structure)
    io.save(pdb_file)

def run_parser(input_file, output_dir):
    """
    Run the results_parser.py over the search file to produce the output summary.
    """
    search_file = input_file + "_search.tsv"  # e.g., 'test.pdb_search.tsv'
    input_path = os.path.join(output_dir, search_file)

    # Debugging: Print constructed path
    print(f"Checking file existence: {input_path}")

    # Check if file exists
    if not os.path.exists(input_path):
        print(f"ERROR: File {input_path} not found. Exiting...")
        return

    cmd = ['python', './results_parser.py', output_dir, search_file]
    print(f'STEP 2: RUNNING PARSER: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()

    print(out.decode("utf-8"))
    if err:
        print(f"Parser Error: {err.decode('utf-8')}")

def run_merizo_search(input_file, id, output_dir):
    output_path = os.path.join(output_dir, id)
    cmd = ['python3',
           '/home/almalinux/EDA1/merizo_search/merizo_search/merizo.py',
           'easy-search',
           input_file,
           '/home/almalinux/EDA1/cath-4.3-foldclassdb',
           output_path,  # Specify output path here
           'tmp',
           '--iterate',
           '--output_headers',
           '-d',
           'cpu',
           '--threads',
           '1'
           ]
    print(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    print(out.decode("utf-8"))
    if err:
        print(f"Merizo Error: {err.decode('utf-8')}")

    # List output directory contents for debugging
    print(f"Contents of output directory {output_dir}:")
    print(os.listdir(output_dir))

    # Check if output file exists
    expected_output = output_path + "_search.tsv"
    if not os.path.exists(expected_output):
        print(f"ERROR: Expected output file {expected_output} not created by merizo.")

def read_dir(input_dir):
    """
    Function reads .pdb and .cif files in the input directory,
    converting .cif to .pdb if needed.
    """
    print("Getting file list")
    file_ids = list(glob.glob(input_dir + "*.pdb")) + list(glob.glob(input_dir + "*.cif"))
    print("Files found:", file_ids)
    analysis_files = []
    for file in file_ids:
        id = file.rsplit('/', 1)[-1]
        # Convert .cif to .pdb
        if file.endswith(".cif"):
            pdb_file = file.replace(".cif", ".pdb")
            convert_cif_to_pdb(file, pdb_file)
            file = pdb_file  # Update file path to the converted .pdb
        analysis_files.append([file, id, sys.argv[2]])
    return analysis_files

def pipeline(filepath, id, outpath):
    # STEP 1
    run_merizo_search(filepath, id, outpath)
    # STEP 2
    run_parser(id, outpath)

if __name__ == "__main__":
    pdbfiles = read_dir(sys.argv[1])
    p = multiprocessing.Pool(10)
    p.starmap(pipeline, pdbfiles)
