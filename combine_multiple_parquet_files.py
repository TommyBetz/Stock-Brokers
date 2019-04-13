# This code combines multiple parquet files. It takes as an input the root folder and the name of an output file.

# Command to run: time spark-submit combine_multiple_parquet_files.py /path_to_input_folder /path_to_output_file.

# Reading all the files.
import sys
import os

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# The input folder which contains multiple parquet files.
root = sys.argv[1]
# The output file.
output = sys.argv[2]

# Walking over the nested structure.
file_names = []
for path, subdirs, files in os.walk(root):
    for name in files:
        file_names.append(os.path.join(path, name))

processing_this_file = 1

# Reading all the parquet files in to a dataframe.
for file_name in file_names:
	if(processing_this_file == 1):
		df_base = spark.read.parquet(file_name)
	else:
		add_df = spark.read.parquet(file_name)
		df_base = df_base.union(add_df)
	processing_this_file = processing_this_file + 1

# Writing the dataframe which contains all the information into a single output parquet file.
df_base.coalesce(1).write.mode('overwrite').parquet('./combined_filtered_data/' + output)

