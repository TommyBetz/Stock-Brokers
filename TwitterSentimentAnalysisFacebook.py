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

sc =SparkContext.getOrCreate()
spark = SparkSession(sc)
#-----------------------------------------
                               
def get_tweet_sentiment(tweet): 
    analysis = TextBlob(tweet)
    # set sentiment 
    # if analysis.sentiment.polarity > 0: 
    #     return 'positive'
    # elif analysis.sentiment.polarity == 0: 
    #     return 'neutral'
    # else: 
    #     return 'negative'
    return analysis.sentiment.polarity

##########################################################
#file:///home/vdhununj
def main(inputs,output):

    # read parquet files
    df_twitter_data = spark.read.parquet('file:///home/vdhununj/BigData2/Project/'+inputs)

    # Facebook word list
    FACEBOOK_Word_list = ['facebook']
    joint_words = ['mark zuckerberg']

    ## UDF to Filter out unwanted tweets##############################################
    @functions.udf(returnType=types.StringType())
    def filter_tweet(tweet_1):
        #tweet_1 = clean_tweet(tweet_1)
        tweet_1 = tweet_1.lower()
        status = 999

        for word in tweet_1.split():
            if word in FACEBOOK_Word_list:
                status = 1
           
        if status != 1:
            for word in joint_words:
                if word in tweet_1:
                    status = 1

        if status == 1:
            return 'FB'

    # clean tweet ###########################################################################
    @functions.udf(returnType=types.StringType())
    def clean_tweet(tweet_1):
            tweet_1 = tweet_1.encode('ascii', 'ignore').decode('ascii')
            return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) |(\w+:\/\/\S+)", " ", tweet_1).split())

    #########################################################################################
    # clean tweets
    df_filtered_data = df_twitter_data.select(df_twitter_data.Date, df_twitter_data.text, df_twitter_data.followers_count, df_twitter_data.favorite_count, df_twitter_data.retweet_count, clean_tweet(df_twitter_data.text).alias('text_clean'))
    # filter tweets
    df_filtered_data = df_filtered_data.select(df_filtered_data.Date, df_filtered_data.text, df_filtered_data.text_clean, filter_tweet(df_filtered_data.text_clean).alias('company'), df_filtered_data.followers_count, df_filtered_data.favorite_count, df_filtered_data.retweet_count)
    #retain only Facebook Tweets
    df_filtered_data = df_filtered_data.filter(df_filtered_data.company=='FB')

    ######################################################################################
    @functions.udf(returnType=types.StringType())
    def process_tweet(tweet):
        #clean_tweet_data = clean_tweet(tweet)
        return get_tweet_sentiment(tweet)

    # generate sentiment
    df_sentiment = df_filtered_data.select(df_filtered_data.Date, df_filtered_data.text, df_filtered_data.text_clean, df_filtered_data.company, process_tweet(df_filtered_data.text_clean).alias('sentiment'),df_filtered_data.followers_count, df_filtered_data.favorite_count, df_filtered_data.retweet_count)
    # cache dateframe
    df_summary = df_sentiment.select(df_sentiment.Date, df_sentiment.company,df_sentiment.sentiment, df_sentiment.followers_count, df_sentiment.favorite_count, df_sentiment.retweet_count).cache()

    # calculate median ############################################################################################################
    df_summary.registerTempTable("medianTbl")
    df_median = spark.sql("SELECT Date, percentile_approx(sentiment, 0.5) as median_sentiment from medianTbl group by Date ")
    spark.catalog.dropTempView("medianTbl")

    df_summary = df_summary.withColumn('weighted_avg_sentiment',df_summary.followers_count * df_summary.sentiment)

    # aggregate counts by Date ####################################################################################################
    df_summary = df_summary.groupby(df_summary.Date).agg(functions.count(df_summary.sentiment).alias('count_tweet'), 
        functions.avg(df_summary.sentiment).alias('avg_sentiment'), 
        functions.sum(df_summary.followers_count).alias('sum_followers_count'), 
        functions.max(df_summary.followers_count).alias('followers_count'), 
        functions.sum(df_summary.favorite_count).alias('sum_favorite_count'), 
        functions.sum(df_summary.retweet_count).alias('sum_retweet_count'),
        functions.sum(df_summary.weighted_avg_sentiment).alias('sum_weighted_avg_sentiment'),
        functions.avg(df_summary.weighted_avg_sentiment).alias('avg_weighted_avg_sentiment_1') # the average of the overall weighted average sentiment values
        )

    # join df_summary with df_median
    df_summary = df_summary.join(df_median, ['Date'])

    # join df_summary with df_sentiment
    df_summary = df_sentiment.join(df_summary, [df_summary.Date==df_sentiment.Date,df_summary.followers_count==df_sentiment.followers_count]).select(df_summary.Date, \
    df_summary.count_tweet,df_summary.avg_sentiment,df_summary.sum_followers_count,df_summary.followers_count.alias('max_followers_count'), df_summary.sum_favorite_count, df_summary.sum_retweet_count, \
    df_summary.median_sentiment, df_summary.sum_weighted_avg_sentiment, df_summary.avg_weighted_avg_sentiment_1,df_sentiment.text.alias('text_original'), df_sentiment.text_clean)

    #calculate avg_weighted_avg_sentiment_2
    df_summary = df_summary.withColumn('avg_weighted_avg_sentiment_2',df_summary.sum_weighted_avg_sentiment/df_summary.sum_followers_count) # the average based on total number of sum_followers

    # sav output in 1 csv file
    df_summary.repartition(1).write.csv(output,header=True)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
