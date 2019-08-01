# Author: Tim Chase
# Date:   July 30, 2019
# Name:   generate_data.py
# Desc:   generate data for load testing of json_to_parquet.py


import os, random, datetime, string, json, argparse


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

def write_multiple_files(basepath, num_files_to_make, num_recs_to_make, yyyy, mm, dd, number_of_days=1, num_dups_to_add=0):
    os.mkdir(basepath)
    # we have to run make_recs all at once to avoid accidental dups across files
    all_recs = make_recs(num_recs_to_make * num_files_to_make, yyyy, mm, dd, number_of_days)
    for i in range(num_files_to_make):
        recs = all_recs[num_recs_to_make * i:num_recs_to_make * (i+1)]
        recs_with_dups = add_dups_to_recs(recs, num_dups_to_add)
        filename = os.path.join(basepath, f'file_{i+1:04}.json')
        # print(filename)
        write_json(recs_with_dups, filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", help="directory in which to create data subdirectory - subdirectory will be created here")
    parser.add_argument("number_of_recs_per_file", help="the number of records total in all data files", type=int)
    parser.add_argument("number_of_files", help="the number of files (max of 9999) ", type=int)
    parser.add_argument("date", help="the date for the ts value as yyyymmdd")
    parser.add_argument("-x", "--number_of_additional_duplicates", help="the number of duplicates to include in addition to number-of-recs", type=int, default=0)
    parser.add_argument("-nd", "--number_of_days", help="the number of days in the future to include in possible ts values (default 1 - same day as date", type=int, default=1)
    args = parser.parse_args()

    if args.directory:
        sub_name = f'jtp_data_{args.number_of_recs_per_file}_by_{args.number_of_files}'
        yyyymmdd = args.date
        yyyy, mm, dd = int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:])
        write_multiple_files(args.directory, args.number_of_files, args.number_of_recs_per_file, yyyy, mm, dd, args.number_of_days)
    
    



