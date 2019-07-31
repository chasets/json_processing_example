# Author: Tim Chase
# Date:   July 30, 2019
# Name:   test_generate_data.py
# Desc:   test generating data for json_to_parquet.py


# TODO: move this to a pytest config; this method _requires_ that pytest be run from project root
import sys, os 
path = os.path.join(os.getcwd(), 'src')
sys.path.append(path)
import generate_data



def test_make_record():
    rec = generate_data.make_record(2019, 5, 1)
    # rec should have 3 keys: id, data, ts
    id = rec['id']
    data = rec['data']
    ts = rec['ts']
    assert 1 == 1

def test_make_recs():
    num_recs = 10
    recs = generate_data.make_recs(num_recs, 2019, 5, 1, 30)
    assert len(recs) == num_recs

def test_add_dups_to_recs():
    original_recs = 200
    additional_recs = 20
    recs = generate_data.make_recs(original_recs, 2017, 12, 1, 90)
    all_recs = generate_data.add_dups_to_recs(recs, additional_recs)
    assert len(all_recs) == original_recs + additional_recs

def test_write_json():
    original_recs = 100
    additional_recs = 22
    filepath = 'data/test_write_json.json'   # TODO: assumptions here
    recs = generate_data.make_recs(original_recs, 2017, 12, 15, 120)
    all_recs = generate_data.add_dups_to_recs(recs, additional_recs)
    generate_data.write_json(all_recs, filepath)
    import json, os
    records_read = json.load(open(filepath))
    os.remove(filepath)
    assert len(records_read['records']) == original_recs + additional_recs




