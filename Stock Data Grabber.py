#!/usr/bin/env python
# coding: utf-8

# Import libraries
import pandas as pd
import json,os,time,quandl

# Get list of companies listed in NASDAQ
# Dataset link: https://www.nasdaq.com/screening/company-list.aspx
# companyInfo = pd.read_csv('data/companylist.csv',sep=',') 

# Get the column containing symbols and convert into list
# stocks = companyInfo['Symbol'].values.tolist()
stocks = ['AAPL','FB']

# Using Quandl to get daily open,high,low and close for all companies listed in NASDAQ
quandl.ApiConfig.api_key = 'USE YOUR OWN API'

for year in range(2012,2019):
    # Get all the data for each year except 2018 and 2019 beacuse Quandl does not provide that data
    dailyPrices = quandl.get_table('WIKI/PRICES', ticker = stocks, 
                    qopts = { 'columns': ['ticker', 'date', 'adj_open','adj_high','adj_low','adj_close','volume'] }, 
                    date = { 'gte': str(year)+'-01-01', 'lte': str(year)+'-12-31' }, 
                    paginate=True)
    dailyPrices.columns = ['Company','Date','Open','High','Low','Close','Volume']
    print('Got data-> From date:' + str(year) + '-01-01','To date:' + str(year) + '-12-31')
    dailyPrices.to_csv('data/stock/'+'data_'+str(year)+'.csv', encoding='utf-8')

    # Free API only allows 20 calls per 10 minutes
    # Adjust time such that the program does not break
    time.sleep(30)

# Get the missing data
# Make sure you have pandas_datareader and fix_yahoo_finance installed
# pandas_datareader: https://pandas-datareader.readthedocs.io/en/latest/
# fix_yahoo_finance: https://pypi.org/project/fix-yahoo-finance/
import fix_yahoo_finance as fyf
from pandas_datareader import data as pdr

# Typical Usage
fyf.pdr_override()

# Get AAPL data
apple_data = pdr.get_data_yahoo('AAPL', start='2018-03-31')
apple_data['Company'] = 'AAPL' 

# Get FB data
fb_data = pdr.get_data_yahoo('FB', start='2018-03-31')
fb_data['Company'] = 'FB' 

# Append the apple and facebook data in one df
combined_data = apple_data.append(fb_data)

# Write to csv
combined_data.to_csv('data/stock/'+'remaining_combined_data.csv', encoding='utf-8')