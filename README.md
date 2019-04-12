# SFU Big Data II Final Project: Stock Market Prediction

The topic of our project is to predict a closing stock price using historic stock data in combination with the sentiments of news articles and twitter tweets. We used an RNN LSTM (Long Short Term Memory) neural network model as this works best for time series data.

## Background, Findings and Details
Further information can be found in our [Poster ADD LINK](http://semver.org/) as well as the [Final Report ADD LINK](http://semver.org/).

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

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

	Comes pre-installed with Anaconda.
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

The News Data is downloaded from this site:

	https://www.kaggle.com/rmisra/news-category-dataset 


### Twitter Data

Instructions & File Name

### Historic Stock Data

Instructions & File Name

## Pre-Processing

### News Data

Instructions & File Name

### Twitter Data

Instructions & File Name

### Historic Stock Data

Instructions & File Name

## Running the LSTM (Long Short Term Memory) Model

Instructions & File Name
Parameter Tweaking


## Visualizations

Instructions & File Name



## Built With

* [Google Cloud Dataproc] (https://cloud.google.com/dataproc/) - Cloud-native Apache Hadoop and Apache Spark)
* [Google Colaboratory (Colab)] (https://colab.research.google.com/) - free Jupyter notebook environment that requires no setup and runs entirely in the cloud
* [Apache Spark] (https://spark.apache.org/) - Apache Spark is a unified analytics engine for big data processing, with built-in modules for streaming, SQL, machine learning and graph processing.
* [Pandas] (https://pandas.pydata.org/) - pandas is an open source, BSD-licensed library providing high-performance, easy-to-use data structures and data analysis tools for the Python programming
* [NumPy] (www.numpy.org/) - NumPy is the fundamental package for scientific computing with Python
* [TextBlob (Simplified Text Processing)] (https://textblob.readthedocs.io/en/dev/) - TextBlob is a Python (2 and 3) library for processing textual data.
* [Keras] (keras.io/) - Keras is an open-source neural-network library written in Python. It is capable of running on top of TensorFlow, Microsoft Cognitive Toolkit, Theano, or PlaidML.
* [Power BI] (https://powerbi.microsoft.com/) - Power BI is a collection of software services, apps, and connectors that work together to turn your unrelated sources of data into coherent, visually immersive, and interactive insights.



## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.


## Authors

* **Gaurav Prachchhak** - *Initial work* - [gauravprachchhak](https://github.com/gauravprachchhak)
* **Mihir Gajjar** - *Initial work* - [GajjarMihir](https://github.com/GajjarMihir)
* **Veekesh Dhununjoy** - *Initial work* - [veekeshjoy](https://github.com/veekeshjoy)
* **Tommy Betz** - *Initial work* - [TommyBetz](https://github.com/TommyBetz)


<!---See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.-->

## References

* REFERENCE 1
* REFERENCE 2

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

