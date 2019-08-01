# Author: Tim Chase
# Date:   July 30, 2019
# Name:   test_process_counts.py
# Desc:   test counts for end-to-end processing via json_to_parquet.py


# TODO: move this to a pytest config; this method _requires_ that pytest be run from project root
import sys, os, pytest 
import os
import pandas as pd

path = os.path.join(os.getcwd(), 'src')
sys.path.append(path)
import generate_data, json_to_parquet


# TODO move all of the setup / tear down stuff to fixtures
# TODO before fixtures, at least refactor setup and tear down here

def test_read_write_simple_file():
    # set up the json data
    original_recs = 10
    json_filepath = 'data/test_read_write_simple_file.json'   # TODO: assumptions here
    parquet_filepath = 'output/test_read_write_simple_file.parquet' 
    recs = generate_data.make_recs(original_recs, 1998, 1, 5, 20)
    generate_data.write_json(recs, json_filepath)

    # process to parquet
    df = json_to_parquet.read_json(json_filepath)
    json_to_parquet.write_parquet(df, parquet_filepath)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(parquet_filepath))

    assert parquet_recs == original_recs

    os.remove(json_filepath)
    os.remove(parquet_filepath)


def test_read_write_with_dups():
    # set up the json data
    original_recs = 100
    additional_recs = 20
    json_filepath = 'data/test_read_write_with_dups.json'   # TODO: assumptions here
    parquet_filepath = 'output/test_read_write_with_dups.parquet' 
    recs = generate_data.make_recs(original_recs, 2019, 8, 1, 365)
    generate_data.write_json(recs, json_filepath)

    # process to parquet
    df = json_to_parquet.read_json(json_filepath)
    df_clean = json_to_parquet.remove_duplicates(df)
    json_to_parquet.write_parquet(df_clean, parquet_filepath)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(parquet_filepath))

    assert parquet_recs == original_recs

    os.remove(json_filepath)
    os.remove(parquet_filepath)

# TODO: parameterize this
@pytest.mark.slow
def test_read_write_with_dups_big():
    # set up the json data
    # 100k = 3 seconds
    # 1m   =  31 seconds
    # 10m  = 468 seconds
    original_recs = 100000
    additional_recs = 2500
    json_filepath = 'data/test_read_write_with_dups_big.json'   # TODO: assumptions here
    parquet_filepath = 'output/test_read_write_with_dups_big.parquet' 
    recs = generate_data.make_recs(original_recs, 2019, 8, 1, 365)
    generate_data.write_json(recs, json_filepath)

    # process to parquet
    df = json_to_parquet.read_json(json_filepath)
    df_clean = json_to_parquet.remove_duplicates(df)
    json_to_parquet.write_parquet(df_clean, parquet_filepath)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(parquet_filepath))

    assert parquet_recs == original_recs

    os.remove(json_filepath)
    os.remove(parquet_filepath)

def test_read_write_multiple_files():
    source_directory = 'data/test1'
    target_file = 'output/test1.parquet'
    original_recs = 1000
    number_of_files = 100

    generate_data.write_multiple_files(source_directory, number_of_files, original_recs, 2019, 1, 15)
    json_to_parquet.process_multiple_files(source_directory, target_file)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(target_file))

    assert parquet_recs == original_recs * number_of_files

    files = os.listdir(source_directory)
    for f in files:
        os.remove(os.path.join(source_directory, f))
    os.rmdir(source_directory)
    os.remove(target_file)

@pytest.mark.slow
def test_read_write_multiple_files_big():
    source_directory = 'data/test2'
    target_file = 'output/test2.parquet'
    original_recs = 1000
    number_of_files = 1000

    generate_data.write_multiple_files(source_directory, number_of_files, original_recs, 2019, 1, 15)
    json_to_parquet.process_multiple_files(source_directory, target_file)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(target_file))

    assert parquet_recs == original_recs * number_of_files

    files = os.listdir(source_directory)
    for f in files:
        os.remove(os.path.join(source_directory, f))
    os.rmdir(source_directory)
    os.remove(target_file)

def test_read_write_multiple_files_with_dups():
    source_directory = 'data/test3'
    target_file = 'output/test3.parquet'
    original_recs = 1000
    additional_recs = 250
    number_of_files = 100

    generate_data.write_multiple_files(source_directory, number_of_files, original_recs, 2019, 1, 15, num_dups_to_add=additional_recs)
    json_to_parquet.process_multiple_files(source_directory, target_file)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(target_file))

    assert parquet_recs == original_recs * number_of_files

    files = os.listdir(source_directory)
    for f in files:
        os.remove(os.path.join(source_directory, f))
    os.rmdir(source_directory)
    os.remove(target_file)

@pytest.mark.slow
def test_read_write_multiple_files_with_dups_big():
    source_directory = 'data/test3'
    target_file = 'output/test3.parquet'
    original_recs = 1000
    additional_recs = 2500
    number_of_files = 1000

    generate_data.write_multiple_files(source_directory, number_of_files, original_recs, 2019, 1, 15, num_dups_to_add=additional_recs)
    json_to_parquet.process_multiple_files(source_directory, target_file)

    # get counts, compare, and clean up
    parquet_recs = len(pd.read_parquet(target_file))

    assert parquet_recs == original_recs * number_of_files

    files = os.listdir(source_directory)
    for f in files:
        os.remove(os.path.join(source_directory, f))
    os.rmdir(source_directory)
    os.remove(target_file)




