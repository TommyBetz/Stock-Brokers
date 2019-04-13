# This is the latest version of the code which preprocesses twitter data. Instead of reading every file as before,
# this code copies every file into a single folder and reads all the parquet files and processes all the data in a
# single run.

# Command to run: time spark-submit Processing_Twitter_Data_version_3.py /path_to_input_folder /path_to_output_folder
# /path_to_intermediate_folder

import datetime
from dateutil.parser import parse
import pyfastcopy

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# # Reading the json data.
# df = spark.read.json('./twitter-2018-10-01/2018/10/01/01/55.json.bz2')

# Reading all the files.
import os
import shutil

# The input root folder which contains the files in a nested structure.
root = sys.argv[1]
# The output folder which should contain the filtered data.
output = sys.argv[2]
# The intermediate folder which will contain all the files from the nested file structure of the data. All the parquet
# files from this folder are loaded into a dataframe and processed in a single run.
dest_dir = sys.argv[3]

# To count the number of files.
counter = 1

# Copying every file.
file_names = []
for path, subdirs, files in os.walk(root):
    for name in files:
	    # Copy the file
    	old_file_name = os.path.join(path, name)
    	shutil.copy(old_file_name, dest_dir)
		
		# Rename the file
    	dest_file = os.path.join(dest_dir, name)
    	new_dest_file_name = os.path.join(dest_dir, name[:-9] + '_' + str(counter) + name[-9:])
    	os.rename(dest_file, new_dest_file_name)
    	os.system("bzip2 -d " + new_dest_file_name)
		# Increment the counter
    	counter = counter + 1
    	print(counter)


print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')
print('Processing the file')
print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')
print('#########################################################################################')

# Read all the parquet files from the intermediate folder.
df = spark.read.json(dest_dir)

# Keeping only the required fields.
df_fields_filtered = df.select(df['timestamp_ms'], df['lang'], df['user'], df['text'], df['favorite_count'], df['retweet_count'])

# Keeping only the tweets in the english language.
df_lang_fields_filtered = df_fields_filtered.filter(df['lang'] == 'en')

# # Dropping the language column.
# df_lang_fields_filtered = df_lang_fields_filtered.drop('lang')

# Filtering the records whose timestamp_ms field is null.
df_time_lang_fields_filtered = df_lang_fields_filtered.filter(df_lang_fields_filtered['timestamp_ms'].isNotNull())

# Extracting the screen_name and followers_count from the user column and deleting the user column.
# df_user_time_lang_fields_filtered = df_time_lang_fields_filtered.withColumn('screen_name', df_time_lang_fields_filtered['user']['screen_name'])
df_time_lang_fields_filtered = df_time_lang_fields_filtered.withColumn('followers_count', df_time_lang_fields_filtered['user']['followers_count'])
# df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.drop('user')

# Converting the timestamp_ms into Date.
@functions.udf(returnType = types.DateType())
def convert_timestamp_to_date(timestamp_in_ms):
	date = datetime.datetime.fromtimestamp(float(timestamp_in_ms)/1000).strftime('%Y-%m-%d')
	return parse(date)

# @functions.udf(returnType = types.DateType())
# def convert_timestamp_to_time(timestamp_in_ms):
# 	ts = int(int(timestamp_in_ms)/1000)
# 	time = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S').split()[1]
# 	return datetime.strptime(time, '%H:%M:%S')

# Converting the timestamp in milliseconds to date.
df_time_lang_fields_filtered = df_time_lang_fields_filtered.withColumn('Date', convert_timestamp_to_date(df_time_lang_fields_filtered['timestamp_ms']))

# Converting the timestamp in milliseconds to time.
# df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.withColumn('Time', convert_timestamp_to_time(df_user_time_lang_fields_filtered['timestamp_ms']))

# Dropping the timestamp_ms column.
# df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.drop('timestamp_ms')

# Reordering the columns.
df_time_lang_fields_filtered = df_time_lang_fields_filtered.select('Date', 'followers_count', 'text', 'favorite_count', 'retweet_count')

null_replace_dictionary = { 'text':'null', 'followers_count':0, 'favorite_count':0, 'retweet_count':0 }

# Replacing the null values in the dataframe so that when we use collect_list we can retain the mapping.
df_time_lang_fields_filtered = df_time_lang_fields_filtered.na.fill(null_replace_dictionary)

# Grouping the tweets by date and aggregating the rest of the columns with collect_list.
# df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.groupby('Date').agg(
#                                     functions.collect_list('screen_name').alias('screen_name'),                                        
#                                     functions.collect_list('followers_count').alias('followers_count'),
#                                     functions.collect_list('Time').alias('Time'),
#                                     functions.collect_list('text').alias('text'),
#                                     functions.collect_list('favorite_count').alias('favorite_count'),
#                                     functions.collect_list('retweet_count').alias('retweet_count'),
#                                     functions.collect_list('possibly_sensitive').alias('possibly_sensitive'),
#                                     functions.collect_list('delete').alias('delete')
#                                     )

# Writing the filtered dataframe as an output.
df_time_lang_fields_filtered.coalesce(4).write.mode('append').parquet(output)



