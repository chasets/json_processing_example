# Author: Tim Chase
# Date:   July 30, 2019
# Name:   test_basic_counts.py
# Desc:   test counts of input json records vs output parquet records


# move this to a fixture later
import json
import pandas as pd

source_file = 'data/original.json'
target_file = 'output/original.parquet'


def test_original_counts():
    # ignore dup case for now
    expected_count = len(json.load(open(source_file))['records'])
    actual_count = len(pd.read_parquet(target_file))
    assert actual_count == expected_count









