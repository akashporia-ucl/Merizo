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
    tmp_dir = "/mnt/minio/temp"  # Specify the writable temp directory

    # Correctly create a mutable environment dictionary
    env = dict(os.environ)
    env['PWD'] = os.getcwd()

    cmd = [
        'python3',
        '/mnt/minio/Merzio/merizo_search/merizo_search/merizo.py',
        'easy-search',
        input_file,
        '/mnt/minio/Merzio/cath-4.3-foldclassdb',
        output_path,
        tmp_dir,  # Pass the writable temporary directory
        '--iterate',
        '--output_headers',
        '-d',
        'cpu',
        '--threads',
        '1'
    ]
    print(f'STEP 1: RUNNING MERIZO: {" ".join(cmd)}')
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
    out, err = p.communicate()
    print(out.decode("utf-8"))
    if err:
        print(f"Merizo Error: {err.decode('utf-8')}")


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
    p = multiprocessing.Pool(4)
    p.starmap(pipeline, pdbfiles)