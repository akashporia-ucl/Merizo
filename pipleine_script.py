from pyspark import SparkContext, SparkConf
import os
from subprocess import Popen, PIPE
from Bio.PDB import MMCIFParser, PDBIO
import sys
import glob

def convert_cif_to_pdb(cif_file, pdb_file):
    try:
        parser = MMCIFParser()
        structure = parser.get_structure("structure", cif_file)
        io = PDBIO()
        io.set_structure(structure)
        io.save(pdb_file)
        print(f"Converted {cif_file} to {pdb_file}")
    except Exception as e:
        print(f"Error converting {cif_file} to {pdb_file}: {e}")

def run_merizo_search(input_file, file_id, output_dir):
    try:
        output_path = os.path.join(output_dir, file_id)
        tmp_dir = "/mnt/minio/temp"  # Temporary directory

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
            print(f"Merizo Error for {input_file}: {err.decode('utf-8')}")
        if out:
            print(f"Merizo Output for {input_file}: {out.decode('utf-8')}")
    except Exception as e:
        print(f"Error running Merizo search for {input_file}: {e}")

def run_parser(file_id, output_dir):
    try:
        search_file = file_id + "_search.tsv"
        input_path = os.path.join(output_dir, search_file)  # Full path to the .tsv file

        if not os.path.exists(input_path):
            print(f"ERROR: Input file {input_path} not found.")
            return

        cmd = ['python3', 'results_parser.py', output_dir, input_path]
        print(f"Running command: {' '.join(cmd)}")
        process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = process.communicate()
        if out:
            print(f"Parser Output for {file_id}: {out.decode('utf-8')}")
        if err:
            print(f"Parser Error for {file_id}: {err.decode('utf-8')}")
    except Exception as e:
        print(f"Error running parser for {file_id}: {e}")

def pipeline(file_data):
    try:
        filepath, file_id, output_dir = file_data
        print(f"Starting pipeline for {file_id}")

        if filepath.endswith(".cif"):
            pdb_file = filepath.replace(".cif", ".pdb")
            convert_cif_to_pdb(filepath, pdb_file)
            filepath = pdb_file  # Update to the converted file

        run_merizo_search(filepath, file_id, output_dir)
        run_parser(file_id, output_dir)

        print(f"Completed pipeline for {file_id}")
    except Exception as e:
        print(f"Pipeline error for {file_data}: {e}")

def read_dir(input_dir, output_dir):
    file_list = glob.glob(os.path.join(input_dir, "*.pdb")) + glob.glob(os.path.join(input_dir, "*.cif"))
    return [(file, os.path.basename(file).split('.')[0], output_dir) for file in file_list]

if __name__ == "__main__":
    conf = SparkConf().setAppName("CIFtoPDBPipeline").setMaster("spark://10.134.12.221:7077")
    sc = SparkContext(conf=conf)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    files = read_dir(input_dir, output_dir)
    num_partitions = min(len(files), 10)
    files_rdd = sc.parallelize(files, numSlices=num_partitions)
    files_rdd.foreach(pipeline)