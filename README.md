
# json_processing_example


Below is a problem statement. 


```json
{
  "records": [
    {
      "id": "10101",
      "data": "ABC",
      "ts": "2019-04-23T17:25:43.111Z"
    },
    {
      "id": "10102",
      "data": "XYZ",
      "ts": "2019-04-23T18:20:32.876Z"
    }
  ]
}
```

```
You are constantly reading files with this format and 
you must persist the records in parquet format.

But we don’t want to duplicate records. If one file is 
duplicated and both are been read, your python code 
should avoid the duplication.

Duplicate records would be something with the same id 
and same ts even if data field is different. We may 
have a lot of records with the same id but all the 
ts must be unique.

Please build a ETL python program that will read all 
the json files in a filesystem path an create parquet 
files that will avoid duplication.

The solution should be high performant and should 
include unit tests.
```

## Initial analysis
1. Read multiple json files; write parquet
2. Handle dups
    a. the phrase "if one file is duplicated" must mean "record" not "file"
    b. "all ts must be unique" = ts is the key
    c. "Duplicate records would be ..." is a specific test case for the keying
    d. if we get a duplicate ts, no indication of whether to update the values or leave the old one, just "avoid the duplication" so we will answer exactly what to do with the duplicates later


## Plan
- We want to use a couple of different techniques / platforms and see what happens when we push the size
- Start with local python using a conda env, pandas, pyarrow
- Pluggable backend so we can switch to spark easily
- property-based testing to generate a bunch of examples
- pytest for sanity checking
- assignment is a one-time run ("read all the json files ...") but how would I implement the duplication logic across multiple runs with no guarantee that dupes couldn't be present on different days?

## Usage
Below, I'll give instructions for using the files in this repository and I'll link to some docs with my thoughts on various topics as I proceeded through development. The final section, [Development progress](#development-progress) was my backlog for adding features. I'm leaving it in this doc so that my development progress can be tracked via the git history for this file. 

### Basics
* Execute `git clone https://github.com/chasets/json_processing_example.git` or unzip the project into a directory.
* `cd json_processing_example` - all commands should be run from this directory.
* Set up a [python environment](doc/environments.md).
* Generate some test data with `python src/generate_data.py data/test0 100 1000 20190501` - this will generate 1000 files with 100 records in each file. Each record will have a random **ts** value for the date May 1, 2019. See additional documentation by executing `python src/generate_data.py -h` and additional usage in [test/test_command_line.py](https://github.com/chasets/json_processing_example/blob/master/test/test_command_line.py)
* Process this data with `python src/json_to_parquet.py data/test0 output test0.parquet`. See additional documentation by executing `python src/json_to_parquet.py -h` and additional usage in [test/](https://github.com/chasets/json_processing_example/tree/master/test). 

### [json_to_parquet.py](https://github.com/chasets/json_processing_example/blob/master/src/json_to_parquet.py)
* [pandas](https://pandas.pydata.org/) does a lot of the heavy lifting here. This allows for rapid development and provides a path forward to [platform portability](doc/platform_expansion_plan.md).
* **read_json** - read the data file into a pandas DataFrame.
* **write_parquet** - after processing write the output data with some options to ease the [path forward](doc/platform_expansion_plan).
* **remove_duplicates** - very straight-forward due to pandas. See the inline comment for my understanding of the business logic here. 
* **process_multiple_files** - process all of the json files in a given directory, writing a single, de-duped parquet output.
* **main** - in order to support both using this file as a library (as in the [tests](https://github.com/chasets/json_processing_example/tree/master/test)) and from the command line, all of the [argparse](doc/command_line.md) features are here.

### [generate_data.py](https://github.com/chasets/json_processing_example/blob/master/src/generate_data.py)
* **make_id**, **make_data**, **make_timestamp**, **make_record** - generate random data based on the format given in the two-record example above. The **ts** can cover a single day or multiple days. I initially planned to use [property-based testing](doc/testing.md) methods to generate data, but [hypothesis](https://hypothesis.readthedocs.io/) was just too slow in generating the amount of data I wanted. 
* **make_recs** - generate the given number of records. There should be no **ts** duplicates here. This turned out to be an issue if the date range is small (a single day) and the number of records is more than a million, in which case there is a pretty good chance of hitting duplicate **ts**. We have to keep track of which **ts** have been generated so we can reject duplicates. 
* **add_dups_to_recs** - we need to test with duplicates so this adds the given number of duplicates to a set of records. See the code for the four types of duplicates that are created. 
* **write_json**, **make_json** - write the given recs out to a file with the example json schema. 
* **write_multiple_files** - this coordinates writing records across multiple files. One nuance here is that the full record set must be created before splitting into multiple files in order to avoid the **ts** duplicate issue discussed above. 
* **main** - in order to support both using this file as a library (as in the [tests](https://github.com/chasets/json_processing_example/tree/master/test)) and from the command line, all of the [argparse](doc/command_line.md) features are here.

### Timing
Below is the timing for the largest set that I tested, fifty million json records (5,000 records in 1,000) files. I ran batches of ten million several times with timing at about 20 minutes. So, xxx for fifty million seems right. 

These were run on a Macbook Air.
```
MacOS 10.14.6
Processor: 2.2 GHz Intel Core i7 
Memory: 8 GB 1600 MHz DDR3
```

`time python src/generate_data.py data/fifty_million_test 50000 1000 20190201 -x 100000 -nd 365`
```
real	36m20.717s
user	20m28.212s
sys	4m32.780s
```
`time python src/json_to_parquet.py data/fifty_million_test output fifty_million_test.parquet`
```

```

### Testing
See the [test/](https://github.com/chasets/json_processing_example/tree/master/test) directory for tests using [pytest](https://docs.pytest.org/en/latest/). I have some [additional thoughts on testing](doc/testing.md).

The tests all build their own input data and clean up after themselves. Several tests are marked `@pytest.mark.slow`. You can skip running those by invoking pytest like this:
`pytest -m "not slow"`

The [command line tests](https://github.com/chasets/json_processing_example/blob/master/test/test_command_line.py) can fail with messages that write to `stdout`, which is normally suppressed by pytest. In order to see `stdout`, invoke pytest like this:
`pytest -s -m "not slow"`

## Development progress 

### Initial features
1. ~~read json~~
2. ~~pandas dataframe~~
3. ~~write parquet~~

* tests:
    - ~~compare parquet records written to json records read~~

### More data, including dups
1. ~~manually mock up data including dup timestamps~~ 
2. ~~handle dups (sort so we control which dups are dropped; maintain portability to spark)~~

Let's not do any partition handling. No need to overcomplicate without knowing more. Write up some notes, but don't assume partition structure.

* ~~add dup tests~~ 

### Generative testing, add multiple file capability
1. ~~functions to generate test data~~ 
2. ~~make command line util for generating test files~~
3. ~~generate a bunch of data~~
4. ~~expand to use multiple files~~
5. ~~add command line capability~~

* ~~tests for multiple files, analysis for dups to expect, update dup tests~~
* add pandasql for testing the expected state 

### Abstract and move to local Spark
1. abstraction filesystem input
2. abstraction for dedup
3. abstraction for writing out files 
4. port to local spark version in Zeppelin (what is timing like for big files?)

* port tests to spark (env check on tests? separate tests?)

### Move to spark cluster
1. stand up EC2 and 3 node EMR cluster 
2. spark-submit with a single node to check timing
3. spark-submit with 3 nodes (how did we scale?)

* test reports to show performance 

























