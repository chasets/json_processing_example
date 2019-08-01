# Author: Tim Chase
# Date:   July 31, 2019
# Name:   test_command_line.py
# Desc:   test the command line version of the generate_data and json_to_parquet


import sys, os, subprocess, pytest
import pandas as pd
sys.path.append('src')
import generate_data, json_to_parquet

def test_cl_generate_small():
    yyyymmdd = '20190501'
    output_dir = 'data/test0'
    num_recs_per_file = '100'
    num_files = '10'

    # subprocess.run(['python', 'src/generate_data.py', output_dir, num_recs_per_file, num_files, yyyymmdd])
    subprocess.run(['python', 'src/generate_data.py', output_dir, num_recs_per_file, num_files, yyyymmdd])

    files = os.listdir(output_dir)
    dfs = []
    for f in files:
        dfs.append(json_to_parquet.read_json(os.path.join(output_dir, f)))
        # clean as we go
        os.remove(os.path.join(output_dir, f))
    os.rmdir(output_dir)
    json_df = pd.concat(dfs)
    
    assert len(json_df) == int(num_recs_per_file) * int(num_files)

@pytest.mark.slow
def test_cl_generate_big():
    yyyymmdd = '20190501'
    output_dir = 'data/test1'
    num_recs_per_file = '10000'
    num_files = '1000'

    subprocess.run(['python', 'src/generate_data.py', output_dir, num_recs_per_file, num_files, yyyymmdd])

    files = os.listdir(output_dir)
    dfs = []
    for f in files:
        dfs.append(json_to_parquet.read_json(os.path.join(output_dir, f)))
        # clean as we go
        os.remove(os.path.join(output_dir, f))
    os.rmdir(output_dir)
    json_df = pd.concat(dfs)
    
    assert len(json_df) == int(num_recs_per_file) * int(num_files)


def test_cl_process_small():
    yyyymmdd = '20190501'
    json_dir = 'data/test0'
    num_recs_per_file = '100'
    num_files = '10'
    parquet_dir = 'output'
    parquet_file = 'test0.parquet'

    subprocess.run(['python', 'src/generate_data.py', json_dir, num_recs_per_file, num_files, yyyymmdd])
    subprocess.run(['python', 'src/json_to_parquet.py', json_dir, parquet_dir, parquet_file])

    assert len(pd.read_parquet(os.path.join(parquet_dir, parquet_file))) == int(num_recs_per_file) * int(num_files)
    
    files = os.listdir(json_dir)
    dfs = []
    for f in files:
        os.remove(os.path.join(json_dir, f))
    os.rmdir(json_dir)
    os.remove(os.path.join(parquet_dir, parquet_file))

@pytest.mark.slow
def test_cl_process_big():
    yyyymmdd = '20190501'
    json_dir = 'data/test1'
    num_recs_per_file = '10000'
    num_files = '1000'
    parquet_dir = 'output'
    parquet_file = 'test0.parquet'

    subprocess.run(['python', 'src/generate_data.py', json_dir, num_recs_per_file, num_files, yyyymmdd])
    subprocess.run(['python', 'src/json_to_parquet.py', json_dir, parquet_dir, parquet_file])

    assert len(pd.read_parquet(os.path.join(parquet_dir, parquet_file))) == int(num_recs_per_file) * int(num_files)
    
    files = os.listdir(json_dir)
    dfs = []
    for f in files:
        os.remove(os.path.join(json_dir, f))
    os.rmdir(json_dir)
    os.remove(os.path.join(parquet_dir, parquet_file)) 


