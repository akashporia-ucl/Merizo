#Update code for spark
from pyspark import SparkContext, SparkConf
import os
from subprocess import Popen, PIPE
from Bio.PDB import MMCIFParser, PDBIO

def convert_cif_to_pdb(cif_file, pdb_file):
    """
    Converts a .cif file to a .pdb file using BioPython's MMCIFParser.
    """
    parser = MMCIFParser()
    structure = parser.get_structure("structure", cif_file)
    io = PDBIO()
    io.set_structure(structure)
    io.save(pdb_file)

def run_merizo_search(input_file, id, output_dir):
    """
    Run the Merizo search process.
    """
    output_path = os.path.join(output_dir, id)
    tmp_dir = "/mnt/minio/temp"  # Temporary directory

    # Environment setup
    env = dict(os.environ)
    env['PWD'] = os.getcwd()

    cmd = [
        'python3',
        '/mnt/minio/Merzio/merizo_search/merizo_search/merizo.py',
        'easy-search',
        input_file,
        '/mnt/minio/Merzio/cath-4.3-foldclassdb',
        output_path,
        tmp_dir,
        '--iterate',
        '--output_headers',
        '-d',
        'cpu',
        '--threads',
        '1'
    ]
    print(f"Running Merizo: {' '.join(cmd)}")
    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
    out, err = process.communicate()
    if err:
        print(f"Merizo Error: {err.decode('utf-8')}")

def run_parser(input_file, output_dir):
    """
    Run the results parser.
    """
    search_file = input_file + "_search.tsv"
    input_path = os.path.join(output_dir, search_file)

    if not os.path.exists(input_path):
        print(f"ERROR: File {input_path} not found.")
        return

    cmd = ['python', './results_parser.py', output_dir, search_file]
    print(f"Running Parser: {' '.join(cmd)}")
    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = process.communicate()
    if err:
        print(f"Parser Error: {err.decode('utf-8')}")

def pipeline(file_data):
    """
    Pipeline to process each file.
    """
    filepath, file_id, output_dir = file_data
    if filepath.endswith(".cif"):
        pdb_file = filepath.replace(".cif", ".pdb")
        convert_cif_to_pdb(filepath, pdb_file)
        filepath = pdb_file  # Update to the converted file
    run_merizo_search(filepath, file_id, output_dir)
    run_parser(file_id, output_dir)

def read_dir(input_dir, output_dir):
    """
    Read input directory for .pdb and .cif files.
    """
    file_list = glob.glob(os.path.join(input_dir, "*.pdb")) + glob.glob(os.path.join(input_dir, "*.cif"))
    return [(file, os.path.basename(file), output_dir) for file in file_list]

if __name__ == "__main__":
    # Initialize Spark
    conf = SparkConf().setAppName("CIFtoPDBPipeline").setMaster("spark://<management-node>:7077")
    sc = SparkContext(conf=conf)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    # Get file list
    files = read_dir(input_dir, output_dir)

    # Parallelize across Spark
    files_rdd = sc.parallelize(files)

    # Run the pipeline
    files_rdd.foreach(pipeline)
