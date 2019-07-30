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
You are constantly reading files with this format and you must persist the records in parquet format.

But we don’t want to duplicate records. If one file is duplicated and both are been read, your python code should avoid the duplication.

Duplicate records would be something with the same id and same ts even if data field is different. We may have a lot of records with the same id but all the ts must be unique.

Please build a ETL python program that will read all the json files in a filesystem path an create parquet files that will avoid duplication.

The solution should be high performant and should include unit tests.
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


### Python environments
It used to be difficult to manage python environments on a single machine for different experimental configurations, and across developers to ensure consistency. `virtualenv` changed that and `conda` environments are an even easier way to handle experimentation and consistency. So, we'll start with a conda environment called `parquet_dev` with Python 3.7, pandas, etc. From there, let's use a local Zeppelin for spark, then (assuming the cost is cool) EMR cluster on AWS. 

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
1. use Hypothesis for generative tests (property-based testing) 
2. make command line util for generating test files
3. generate a bunch of data
4. expand to use multiple files
5. add command line capability
6. how much data can we get in a 10 or 20 minute run

* tests for multiple files, analysis for dups to expect, update dup tests

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
























