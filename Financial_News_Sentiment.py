# Financial times dataset 01-2018 to 05-2018from pyspark.context import SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import SparkSession
from pyspark import SparkContext


# sc = SparkContext('local')
# spark = SparkSession(sc)

sc =SparkContext.getOrCreate()
spark = SparkSession(sc)

lst_companies = ['apple','facebook']

def organizationsAndsentiment(x):
	entities = []
	for i in range(len(x)):
		entity = x[i]
		entities += entity
	return entities

def sentiment(x):
	str_company = x[0]
	lst = x[1].split(',')
	lst_temp = lst[0]
	str_sentiment = ''
	for i in range(0, len(lst), 2):
		if str_company in lst[i]:
			str_sentiment = lst[i+1]
			break
	if 'positive' in str_sentiment:
		return 1
	if 'negative' in str_sentiment:
		return -1
	else:
		return 0


def main(inputs,output):
	
	count = 0

	for company in lst_companies:
		#print(company)
		count = count + 1
		# READ DATA
		df_temp = spark.read.json('file:///home/vdhununj/BigData2/Project/Financial_News/'+inputs)
		
		# ADD SHARES_LIKES COLUMN
		df_temp = df_temp.withColumn('shares_likes', col('thread.social.facebook.shares') + col('thread.social.gplus.shares') + \
					   col('thread.social.pinterest.shares') + col('thread.social.vk.shares') + \
					   col('thread.social.linkedin.shares') + col('thread.social.stumbledupon.shares'))
		
		# ADD ENTITY/SENTIMENT COLUMN
		organizationsAndsentiment_udf = udf(lambda z: organizationsAndsentiment(z), StringType())
		df_temp = df_temp.withColumn('organizations_sentiment', organizationsAndsentiment_udf('entities.organizations'))
		
		# MAKE SELECTION
		df_temp = df_temp.select('published', 'organizations_sentiment', 'shares_likes', 'thread.performance_score', 'thread.replies_count', 'title', 'text')
		df_temp = df_temp.withColumn('date', df_temp.published.substr(1, 10))
		df_temp = df_temp.drop('published')
		
		# FILTER FOR COMPANY
		df_temp = df_temp.where(df_temp.organizations_sentiment.contains(company))
		
		# ADD COMPANY NAME COLUMN
		df_temp = df_temp.withColumn('company', lit(company))
		
		# ADD SENTIMENT COLUMN
		sentiment_udf = udf(lambda z: sentiment(z), StringType())
		df_temp = df_temp.withColumn('sentiment', sentiment_udf(array(df_temp.company, df_temp.organizations_sentiment)))
		df_temp = df_temp.drop('organizations_sentiment')
		
		print(df_temp.count())
		#ADD FINAL DF
		if count == 1:
			df_final = df_temp
		else:
			df_final = df_final.union(df_temp)
			
		
		#df_final.show()
		
	# CALCULATE MEDIAN

	df_final.registerTempTable("medianTbl")
	df_median = spark.sql("SELECT date, company, percentile_approx(sentiment, 0.5) as median_sentiment from medianTbl group by date, company ")
	spark.catalog.dropTempView("medianTbl")
		
	# df_summary.select('median_sentiment_apple').show()
	#df_median.show()

	###### AGGREGATIONS ###################################################################

	## WEIGHTED SENTIMENTS
	df_sentiment = df_final.withColumn('weighted_sentiment', df_final.shares_likes * df_final.sentiment)
	# df_sentiment.select(df_final.shares_likes, df_final.sentiment, 'weighted_sentiment').show()
	#df_sentiment.show()

	#### CALCULATE AGGREGATIONS

	df_summary = df_sentiment

	df_summary = df_summary.groupby(df_summary.date, df_summary.company).agg(functions.count(df_summary.sentiment).alias('count_articles'), 
			functions.avg(df_summary.sentiment).alias('avg_sentiment'), 
			functions.sum(df_summary.shares_likes).alias('sum_shares_likes'), 
			functions.max(df_summary.shares_likes).alias('max_shares_likes'), 
			functions.sum(df_summary.weighted_sentiment).alias('sum_weighted_sentiment'),
			functions.avg(df_summary.weighted_sentiment).alias('avg_weighted_sentiment_1') # the average of the overall weighted average sentiment values
			)

	#df_summary.show()

	### JOIN MEDIAN

	df_summary = df_summary.join(df_median, ['date', 'company'])
	#df_summary.show()

	df_summary2 = df_summary.withColumn('avg_weighted_sentiment_2', \
		df_summary.sum_weighted_sentiment / df_summary.count_articles)

	#df_summary2.show()

	df_summary2 = df_summary2.join(df_sentiment, [df_summary.date == df_sentiment.date, \
		df_summary.company == df_sentiment.company, df_sentiment.shares_likes == df_summary.max_shares_likes]) \
		.select(df_summary.date, df_summary.company, df_summary.count_articles, df_summary.sum_shares_likes, \
		df_summary.max_shares_likes, df_summary.median_sentiment, df_summary.avg_sentiment, \
		df_summary.sum_weighted_sentiment, df_summary.avg_weighted_sentiment_1, df_summary2.avg_weighted_sentiment_2,\
		df_sentiment.title, df_sentiment.text)

	#df_summary2.show()
	df_summary2 = df_summary2.dropDuplicates(['date','company'])

	df_summary3 = df_summary2.toPandas()

	df_summary3.to_csv(output+".csv", index=False)


if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	main(inputs,output)
