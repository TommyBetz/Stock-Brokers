# Command to run: time spark-submit Processing_Twitter_Data_version_1.py /path_to_input_folder

import datetime
from dateutil.parser import parse

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

# The input root folder which contains the files in a nested structure.
root = sys.argv[1]

# Walking over the nested structure.
file_names = []
for path, subdirs, files in os.walk(root):
    for name in files:
        file_names.append(os.path.join(path, name))

total_number_of_file_names = len(file_names)

processing_this_file = 1

# Processing every file.
for path, subdirs, files in os.walk(root):
	for index, name_of_file in enumerate(files):
		print('#########################################################################################')
		print('#########################################################################################')
		print('#########################################################################################')
		print('#########################################################################################')
		print('#########################################################################################')
		print('Processing' + ' ' + str(processing_this_file) + '/' + str(total_number_of_file_names) + ' ' + 'files')
		print('#########################################################################################')
		print('#########################################################################################')
		print('#########################################################################################')
		print('#########################################################################################')
		print('#########################################################################################')

		# Load the json data file in to a dataframe.
		df = spark.read.json(('file:///' + path +'/' + name_of_file))

		# Keeping only the required fields.
		df_fields_filtered = df.select(df['user'], df['timestamp_ms'], df['lang'], df['text'], df['favorite_count'], df['retweet_count'], df['possibly_sensitive'], df['delete'])

		# Keeping only the tweets in the english language.
		df_lang_fields_filtered = df_fields_filtered.filter(df['lang'] == 'en')

		# Dropping the language column.
		df_lang_fields_filtered = df_lang_fields_filtered.drop('lang')

		# Filtering the records whose timestamp_ms field is null.
		df_time_lang_fields_filtered = df_lang_fields_filtered.filter(df_lang_fields_filtered['timestamp_ms'].isNotNull())

		# Extracting the screen_name and followers_count from the user column and deleting the user column.
		df_user_time_lang_fields_filtered = df_time_lang_fields_filtered.withColumn('screen_name', df_time_lang_fields_filtered['user']['screen_name'])
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.withColumn('followers_count', df_user_time_lang_fields_filtered['user']['followers_count'])
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.drop('user')

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
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.withColumn('Date', convert_timestamp_to_date(df_user_time_lang_fields_filtered['timestamp_ms']))

		# Converting the timestamp in milliseconds to time.
		# df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.withColumn('Time', convert_timestamp_to_time(df_user_time_lang_fields_filtered['timestamp_ms']))

		# Dropping the timestamp_ms column.
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.drop('timestamp_ms')

		# Reordering the columns.
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.select('Date', 'screen_name', 'followers_count', 'text', 'favorite_count', 'retweet_count', 'possibly_sensitive', 'delete')

		# Adding the possibly_sensitive column.
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.withColumn('possibly_sensitive', df_user_time_lang_fields_filtered['possibly_sensitive'].cast(types.StringType()))

		null_replace_dictionary = { 'screen_name':'null', 'text':'null', 'possibly_sensitive':'null',
		                            'followers_count':0, 'favorite_count':0, 'retweet_count':0}

		# Replacing the null values in the dataframe so that when we use collect_list we can retain the mapping.
		df_user_time_lang_fields_filtered = df_user_time_lang_fields_filtered.na.fill(null_replace_dictionary)

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
		df_user_time_lang_fields_filtered.coalesce(1).write.mode('append').parquet('./filtered_data/')
		processing_this_file = processing_this_file + 1