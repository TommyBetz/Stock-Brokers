# Huffington Post News Data - (2012-2016)
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
from pyspark import SparkContext
import re 
from textblob import TextBlob 
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, functions, types
import pandas as pd
import glob

sc =SparkContext.getOrCreate()
spark = SparkSession(sc)

###################################################
def main(inputs, output):

	news_schema = types.StructType([
	types.StructField('id', types.IntegerType(), False),
	types.StructField('id2', types.IntegerType(), False),	
	types.StructField('authors', types.StringType(), False),
	types.StructField('category', types.StringType(), False),
	types.StructField('date', types.StringType(), False),
	types.StructField('headline', types.StringType(), False),
	types.StructField('link', types.StringType(), False),
	types.StructField('short_description', types.StringType(), False),
	types.StructField('body', types.StringType(), False),])

	pd.set_option('display.max_colwidth', -1)
	# read multiple csv files
	news_data = pd.concat([pd.read_csv(f) for f in glob.glob(inputs + '/*.csv')], ignore_index = True)
	# create pyspark dataframe
	df_news_data = spark.createDataFrame(news_data,schema=news_schema)

	#### GET Facebook Related words 
	FACEBOOK_Word_list = ['facebook']
	joint_words = ['mark zuckerberg']

 	## UDF to Filter out unwanted news ##############################################
	@functions.udf(returnType=types.StringType())
	def filter_appl_news(body):

		body = body.lower()

		status = 999

		for word in body.split():
			if word in FACEBOOK_Word_list:
				status = 1

		if status != 1:
			for word in joint_words:
				if word in body:
 					status = 1

		if status == 1:
			return 'FB'

	####################################################
	@functions.udf(returnType=types.StringType())
	def count_company_frequency(body):
		body = body.lower()
		count = 0
		for i in FACEBOOK_Word_list:
			count = body.count(i) + count
			# if i in APPLE_Word_List:
		return count

	###################################################

	# clean body ###########################################################################
	@functions.udf(returnType=types.StringType())
	def clean_text(body):
		body = body.encode('ascii', 'ignore').decode('ascii')
		return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", body).split())

	###################################################
	@functions.udf(returnType=types.FloatType())
	def process_body(text):
		analysis = TextBlob(text)
		return analysis.sentiment.polarity
	###################################################
	# filter only facebook related news
	df_headline = df_news_data.withColumn('company', filter_appl_news(df_news_data.body))
	# retain FB related records only
	df_frequency = df_headline.filter(df_headline.company == 'FB')
	# count the number of times a the facebook related word is found in the body
	df_frequency = df_frequency.withColumn('company_frequency', count_company_frequency(df_headline.body)) 
	# retain the records if at least 1 facebook related word is present
	df_frequency = df_frequency.filter(df_frequency.company_frequency >=1)
	# clean body text
	df_cleaned = df_frequency.withColumn('body_clean', clean_text(df_frequency.body))
	# clean headline text
	df_cleaned = df_cleaned.withColumn('headline_clean', clean_text(df_frequency.headline))
	# generate text sentiment from body_clean
	df_sentiment = df_cleaned.withColumn('sentiment', process_body(df_cleaned.body_clean))

	# calculate median ############################################################################################################
	df_sentiment.registerTempTable("medianTbl")
	df_median = spark.sql("SELECT date, percentile_approx(sentiment, 0.5) as median_sentiment from medianTbl group by date ")
	spark.catalog.dropTempView("medianTbl")

	###### AGGREGATIONS ###################################################################

	## WEIGHTED SENTIMENTS
	df_sentiment = df_sentiment.withColumn('weighted_sentiment', df_sentiment.company_frequency * df_sentiment.sentiment)

	# aggregations to generate features grouped by date
	df_summary = df_sentiment.groupby(df_sentiment.date).agg(functions.count(df_sentiment.sentiment).alias('count_news'), 
	functions.avg(df_sentiment.sentiment).alias('avg_sentiment'), 
	functions.sum(df_sentiment.company_frequency).alias('sum_company_frequency'), 
	functions.max(df_sentiment.company_frequency).alias('max_company_frequency'), 
	functions.sum(df_sentiment.weighted_sentiment).alias('sum_weighted_sentiment'),
	functions.avg(df_sentiment.weighted_sentiment).alias('avg_weighted_sentiment_1') 
	)

	# calculate avg_weighted_sentiment_2
	df_summary = df_summary.withColumn('avg_weighted_sentiment_2', df_summary.sum_weighted_sentiment/df_summary.sum_company_frequency)

	# join df_summary and df_sentiment
	df_summary = df_sentiment.join(df_summary, [df_summary.date==df_sentiment.date,df_summary.max_company_frequency==df_sentiment.company_frequency]).select(df_summary.date, \
	df_summary.count_news,df_summary.avg_sentiment,df_summary.sum_company_frequency,df_summary.max_company_frequency, \
	df_summary.sum_weighted_sentiment, df_summary.avg_weighted_sentiment_1,df_summary.avg_weighted_sentiment_2,df_sentiment.headline.alias('headline_original'), df_sentiment.headline_clean)

	# join the df_summary dataframe with df_median on date field
	df_summary = df_summary.join(df_median, ['date'])

	# Select the required fields
	df_summary = df_summary.select(df_summary.date, \
	df_summary.count_news,df_summary.avg_sentiment,df_summary.median_sentiment, df_summary.sum_company_frequency,df_summary.max_company_frequency, \
	df_summary.sum_weighted_sentiment, df_summary.avg_weighted_sentiment_1,df_summary.avg_weighted_sentiment_2, \
	df_summary.headline_original, df_summary.headline_clean)
	
	# drop duplicates (if any)
	df_summary = df_summary.dropDuplicates(['date'])

	# save the output as 1 csv file
	df_summary.repartition(1).write.csv(output,header=True)

	###################################################
if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs,output)

