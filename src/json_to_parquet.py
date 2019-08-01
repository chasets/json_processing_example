# Author: Tim Chase
# Date:   July 30, 2019
# Name:   json_to_parquet.py
# Desc:   Convert json input files to parquet. See https://github.com/chasets/json_processing_example

import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse, os


def read_json(filename):
    recs = json.load(open(filename))['records']
    df = pd.DataFrame(recs)
    return df


def write_parquet(df, output_filename):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filename, flavor='spark', coerce_timestamps='ms')


def remove_duplicates(df):
    # The problem statement says "Duplicate records would be something with the same id and same ts even if data field is different. We may have a lot of records with the same id but all the ts must be unique." 
    # So, drop_duplicates should work for pandas and dropDuplicates should work for spark. 
    # Although we don't have a spec on _which_ dups to drop, I don't like it to be arbitrary. So, sort by id and data within 
    # the ts (for lack of any other rule) and keep the first one
    df.sort_values(by=['ts', 'id', 'data'], inplace=True)
    df.drop_duplicates(subset=['ts'], keep='first', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def process_multiple_files(source_dir, target_file):
    files = os.listdir(source_dir)
    dfs = []
    for f in files:
        dfs.append(read_json(os.path.join(source_dir, f)))
    json_df = pd.concat(dfs)
    dedup_df = remove_duplicates(json_df)
    write_parquet(dedup_df, target_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("source_directory", help="directory to the json files to process")
    parser.add_argument("target_directory", help="parquet output directory")
    parser.add_argument("target_file", help="parquet output filename")
    args = parser.parse_args()

    target_file = os.path.join(args.target_directory, args.target_file)
    process_multiple_files(args.source_directory, target_file)    



