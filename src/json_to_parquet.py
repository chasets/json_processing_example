# Author: Tim Chase
# Date:   July 30, 2019
# Name:   json_to_parquet.py
# Desc:   Convert json input files to parquet. See https://github.com/chasets/json_processing_example

import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


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

if __name__ == '__main__':
    source = 'data/dup_data1.json'
    target = 'output/dup_data1.json'
    json_df = read_json(source)
    dedup_df = remove_duplicates(json_df)
    write_parquet(dedup_df, target)



