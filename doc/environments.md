# Python environments

It used to be difficult to manage python environments on a single machine for different experimental configurations, and across developers to ensure consistency. `virtualenv` changed that and `conda` environments are an even easier way to handle experimentation and consistency. So, we'll start with a conda environment called `parquet_dev` with Python 3.7, pandas, etc. From there, the next step would be to use a local Zeppelin for spark, then an EMR cluster on AWS. 




