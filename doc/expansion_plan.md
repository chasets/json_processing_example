# Plan for extending this processing
There isn't a lot to be gained from overly optimizing this toy example to run extremely fast on my desktop machine. I can process 50 million records in about 3 hours. I'm fine with that. If I had more input or needed better times, I would tend to move toward [spark](https://spark.apache.org/) for processing. In my [initial plan](https://github.com/chasets/json_processing_example#plan), I gave the generaly direction for expansaion and here, I'll add some details. 

Pandas is an easy choice for the assignment of "turn some json into parquet". There is a good parquet library, but more importantly, pandas uses several abstrations in common with spark. Expanding on the README, here is how I would proceed in adding spark capabilites. 

## Abstraction
Of course, reading and writing files is a common abstraction. Pandas and spark also both share a `DataFrame` abstraction. However, I chose the method of using [pandas.DataFrame.drop_duplicates](https://pandas.pydata.org/pandas-docs/version/0.17/generated/pandas.DataFrame.drop_duplicates.html) for handiling the `deduplication` requirement specifically because it has a good analogy in [DataFrame.dropDuplicate](https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/DataFrame.html#dropDuplicates(java.lang.String[]). 

My thinking has been influenced by the [web2py Database Abstraction Layer](http://www.web2py.com/books/default/chapter/29/06/the-database-abstraction-layer). Here is how I would proceed in making the current code more generic. 

1. DataFrame class
Python's object oriented functionality is not the right choice for all problems, but I think it is here. It would only take a very lightweight class on top of the pandas DataFrame to provide for reading, writing, and removing duplicates. 
2. Backend config system
We need some method for telling our custom DataFrame class to use pandas (for example) to actually process data. Both web2py and [Django](https://www.djangoproject.com/) have good examples for this. 
3. Migration
This first step is only about using pandas to power our custom DataFrame class so migrating should be straight-forward. The current tests will support that migration nicely. 

## Adding Spark Backend
It is challenging to get a usable spark environment up and running. I really like [Apache Zeppelin](https://zeppelin.apache.org/). There is a single download and it is very straight-forward to get a Zeppelin instance running locally with a PySpark notebook. This is the place to work on adding the `spark` backend to the custom DataFrame. 

I haven't use pytest with Zeppelin. Getting the tests to work and adding tests would probably be more time-consuming that actually getting the `spark` backend coded and configured. I would look at the performance here out of curiosity, but I wouldn't try to optimize anything. Zeppelin on a desktop is going to run in [spark stand-alone](http://spark.apache.org/docs/latest/spark-standalone.html) mode. There is no benefit in spending time optimizing here. 

## Moving to EMR
AWS EMR is a great place to test this kind of use case. The next step would be to set up a 3 node EMR cluster. Install this package on the Master node. Run a `spark-submit` in standalone mode, which should be very similar to how we were running on the desktop using Zeppelin. Once that works, submit in `cluster` mode. We should see better than 2x performance improvement right away. Use the [Spark History Server](https://spark.apache.org/docs/latest/monitoring.html) and the [YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) tools to make sure that we are really using all of our node resources. Just guessing but I don't see why this whole experiment would need to cost more than a few dollars in EMR resources. 






