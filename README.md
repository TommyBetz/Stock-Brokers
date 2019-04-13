# SFU Big Data II Final Project: Stock Market Predictions

The topic of our project is to predict a closing stock price using historic stock data in combination with the sentiments of news articles and twitter tweets. We used an RNN LSTM (Long Short Term Memory) neural network model as this works best for time series data.

## Background, Findings and Details
Further information can be found in our 
 - [Poster](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/submitted_documents/poster.pdf)
 - [Final Report](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/submitted_documents/report.pdf)
 - [Video](https://www.youtube.com/watch?v=8l-cNF2IGjU)

## Getting Started ADD

<!--These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.-->

### Requisites

Our code is written in Python and executed on Google Google Cloud Dataproc (Cloud-native Apache Hadoop and Apache Spark) and Google Colab. Given the required libraries are installed, this code can also be run locally. However, as the twitter dataset reaches 3 TB, we recommend Google Dataproc for downloading and pre-processing such large data volume.

For the visualizations part we used Power BI.

As we used Google Dataproc and Google Colab, there was no need to install any additional packages and libraries. However, if you would like to run this code locally, these are the necessaties to install:

#### Cloud Prerequisites

To run the code on cloud computing:

Google Cloud Dataproc] - Cloud-native Apache Hadoop and Apache Spark
	https://cloud.google.com/dataproc/

Google Colaboratory (Colab) - free Jupyter notebook environment that requires no setup and runs entirely in the cloud
	https://colab.research.google.com/

#### Local Prerequisites

For a local installation, we recommend using Anaconda:

Download and install Anaconda

	https://www.anaconda.com/distribution/


Jupyter Notebook

	comes pre-installed with Anaconda.
<!---pip3 install --upgrade pip-->



PySpark

	conda install -c conda-forge pyspark
<!---pip install pyspark-->


TextBlob

	conda install -c conda-forge textblob 
<!---pip install -U textblob-->


Pandas

	conda install -c anaconda pandas
<!---pip install pandas-->


Glob Python3

	conda install -c conda-forge glob2 
<!---sudo pip install glob3-->

Download and install Power BI

	https://powerbi.microsoft.com/en-us/downloads/


## Getting the Data

Please follow these instructions to obtain the raw datasets.

### News Data

The [News Data](https://www.kaggle.com/rmisra/news-category-dataset) is grabbed from [Kaggle](https://www.kaggle.com/datasets). 

### Twitter Data

The [Twitter Data](https://archive.org/search.php?query=collection%3Atwitterstream&sort=-publicdate&page=1) is collected from the Internet Archive collection of twitter stream.

We then downloaded all datasets onto the cluster and ran FILE to ..... *EDIT*

### Historic Stock Data

We get our stock data from [Quandl API](https://www.quandl.com/) and [Yahoo Finance using Pandas Datareader](https://pandas-datareader.readthedocs.io/en/latest/).

You can find the code that gets this data [here](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/Stock%20Data%20Grabber.py).

## Pre-Processing

### News Data

For Apple:
	[HuffingtonNewsSentimentApple.py](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/HuffingtonNewsSentimentApple.py)
```
$ python3 HuffingtonNewsSentimentApple.py <INPUT_PATH> <OUTPUT_PATH>
```
For Facebook:
	[HuffingtonNewsSentimentFacebook.py](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/HuffingtonNewsSentimentFacebook.py)
```
$ python3 HuffingtonNewsSentimentFacebook.py <INPUT_PATH> <OUTPUT_PATH>
```
`INPUT_PATH`: path to the directory containing all csv files.

`OUTPUT_PATH`: file path where output csv file will be written.


### Twitter Data

For pre-processing Twitter data: Processing_Twitter_Data_version_1.py

```
$ time spark-submit Processing_Twitter_Data_version_1.py /path_to_input_folder
```
`path_to_input_folder`: path to the directory containing all nested JSON files.


For pre-processing Twitter data: Processing_Twitter_Data_version_2.py

```
$ time spark-submit Processing_Twitter_Data_version_2.py /path_to_input_folder /path_to_output_folder
```
`path_to_input_folder`: path to the directory containing all nested JSON files.

`path_to_output_folder`: file path where output parquet files will be written.


For pre-processing Twitter data: Processing_Twitter_Data_version_3.py

```
$ time spark-submit Processing_Twitter_Data_version_3.py /path_to_input_folder /path_to_output_folder /path_to_intermediate_folder
```
`path_to_input_folder`: path to the directory containing all nested JSON files.

`path_to_output_folder`: file path where output parquet files will be written.

`path_to_intermediate_folder`: file path contains all the files from the nested file structure.

For Apple:
	[TwitterSentimentAnalysisApple.py](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/TwitterSentimentAnalysisApple.py)
```
$ python3 TwitterSentimentAnalysisApple.py <INPUT_PATH> <OUTPUT_PATH>
```

For Facebook:
	[TwitterSentimentAnalysisFacebook.py](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/TwitterSentimentAnalysisFacebook.py)
```
$ python3 TwitterSentimentAnalysisFacebook.py <INPUT_PATH> <OUTPUT_PATH>
```
`INPUT_PATH`: path to the directory containing all parquet files.

`OUTPUT_PATH`: file path where output csv file will be written.


To combine multiple parquet files: combine_multiple_parquet_files.py
```
$ time spark-submit combine_multiple_parquet_files.py /path_to_input_folder /path_to_output_file
```

`path_to_input_folder`: path to the directory containing all parquet files.

`path_to_output_file`:  file path where output parquet file will be written.

### Historic Stock Data

All the preprocessing tasks for the stock data were handled during data collection only.

## Running the LSTM (Long Short Term Memory) Model

[LSTM_Model.ipynb](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/LSTM_Model.ipynb)

It contains the LSTM model developed using Keras.

## Visualizations

We created our visualizations in the free version of Power BI. As we do not have a license, we are not able to provide a file to share. However, the visualizations can be found in the [PDF print out](https://github.com/gauravprachchhak/Stock-Brokers/blob/master/submitted_documents/StockPredictionResults.pdf).


## Built With

* **Google Cloud Dataproc** https://cloud.google.com/dataproc/ - Cloud-native Apache Hadoop and Apache Spark
* **Google Colaboratory (Colab)** https://colab.research.google.com/ - free Jupyter notebook environment that requires no setup and runs entirely in the cloud
* **Apache Spark** https://spark.apache.org/ - Apache Spark is a unified analytics engine for big data processing, with built-in modules for streaming, SQL, machine learning and graph processing.
* **Pandas** https://pandas.pydata.org/ - pandas is an open source, BSD-licensed library providing high-performance, easy-to-use data structures and data analysis tools for the Python programming
* **NumPy** www.numpy.org/ - NumPy is the fundamental package for scientific computing with Python
* **TextBlob (Simplified Text Processing)** https://textblob.readthedocs.io/en/dev/ - TextBlob is a Python (2 and 3) library for processing textual data.
* **Keras** keras.io/ - Keras is an open-source neural-network library written in Python. It is capable of running on top of TensorFlow, Microsoft Cognitive Toolkit, Theano, or PlaidML.
* **Power BI** https://powerbi.microsoft.com/ - Power BI is a collection of software services, apps, and connectors that work together to turn your unrelated sources of data into coherent, visually immersive, and interactive insights.



<!--## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.-->


## Authors

* **Gaurav Prachchhak** - [gauravprachchhak](https://github.com/gauravprachchhak)
* **Mihir Gajjar** - [GajjarMihir](https://github.com/GajjarMihir)
* **Veekesh Dhununjoy** - [veekeshjoy](https://github.com/veekeshjoy)
* **Tommy Betz** - [TommyBetz](https://github.com/TommyBetz)


<!---See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.-->

## References

* [**Advances in Financial Machine Learning**](https://www.wiley.com/en-aw/Advances+in+Financial+Machine+Learning-p-9781119482086)
* [**Datacamp – Stock Market Prediction**](https://www.datacamp.com/community/tutorials/lstm-python-stock-market)
* **Github Repositories** – [LinuxIsCool](https://github.com/LinuxIsCool), [Bidirectional LSTM](https://github.com/keras-team/keras/issues/1629)


<!---## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc-->
