# Author: Tim Chase
# Date:   July 30, 2019
# Name:   generate_data.py
# Desc:   generate data for load testing of json_to_parquet.py


import random, datetime, string, json

def make_id():
    return f'{random.randrange(1,99999):05}'

def make_data():
    return ''.join(random.choices(string.ascii_uppercase, k=3))

def make_timestamp(yyyy, mm, dd, number_of_days=1):
    # return a timestamp in the example json format
    # timestamp is a random time on that date or a random number of days in the future out to the given number_of_days
    start_date = datetime.datetime(yyyy, mm, dd)
    future_date = start_date + datetime.timedelta(random.randrange(0, number_of_days))
    future_datetime = future_date + random.random() * datetime.timedelta(days=1)
    future_iso = future_datetime.isoformat()[:-3] + "Z"
    return future_iso

def make_record(yyyy, mm, dd, number_of_days=1):
    return { "id" : make_id(), "data" : make_data(), "ts" : make_timestamp(yyyy, mm, dd, number_of_days=number_of_days) }


def make_recs(num_recs_to_make, yyyy, mm, dd, number_of_days=1):
    recs = []
    keys = {}
    for i in range(num_recs_to_make):
        rec = make_record(yyyy, mm, dd, number_of_days=number_of_days)
        # if the number of days is low, there is a chance for a ts collision, so toss out ts that already exist
        key = rec['ts']
        while 1:
            try:
                keys[key]
                # print("colliding on " + key)
                rec = make_record(yyyy, mm, dd, number_of_days=number_of_days)
                key = rec['ts']
            except:
                recs.append(rec)
                keys[key] = 1
                break
    return recs

def add_dups_to_recs(recs, num_dups_to_add):
    # dups based on ts; 4 types of dups: 
    #       full
    #       same ts different id
    #       same ts different data
    #       same ts different data and id
    #
    # for each type, replace the data by calling the appropriate function above to get new data; _technically_ we could have
    # a collision and randomly get the same data we are replacing. This is very unlikely and doesn't change the properites of
    # what we are trying to do here, so ignore it. 
    total_out = len(recs) + num_dups_to_add
    out_recs = recs[:]      # start with the original recs; we are _adding_ 
    # round robin add in same order as given above
    for i in range(len(recs)):
        if i % 4 == 0: out_recs.append(recs[i])
        elif i % 4 == 1: out_recs.append( { 'id':make_id(), 'data':recs[i]['data'], 'ts':recs[i]['ts'] } )
        elif i % 4 == 2: out_recs.append( { 'id':recs[i]['id'], 'data':make_data(), 'ts':recs[i]['ts'] } )
        elif i % 4 == 3: out_recs.append( { 'id':make_id(), 'data':make_data(), 'ts':recs[i]['ts'] } )
        if len(out_recs) >= total_out: break

    return out_recs[:total_out]


def write_json(recs, filepath):
    json.dump({"records" : recs}, open(filepath, 'w'))


def make_json(recs):
    return json.dumps( {"records" : recs} )


