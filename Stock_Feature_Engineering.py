#!/usr/bin/env python
# coding: utf-8

# Import Libraries
import pandas as pd
import os

# Get the path to data directory
path = os.path.abspath('data')

# Combine all stock data files
new_data = pd.DataFrame([])
for file in os.listdir(path+'/stock'):
    data = pd.read_csv(file)
#     # Extract only AAPL data
#     apple_data = data[(data['Company']=='AAPL')]
    # Extract only FB data
    fb_data = data[(data['Company']=='FB')]
#     # Append AAPL data
#     new_data = new_data.append(apple_data)
    # Append FB data
    new_data = new_data.append(fb_data)

# Selecting
new_data = new_data[['Date','Open','High','Low','Close','Volume']]

# Get VWAP i.e. Volume Weighted Average Price
new_data['Cum_Vol'] = new_data['Volume'].cumsum()
new_data['Cum_Vol_Price'] = (new_data['Volume'] * (new_data['Open'] + new_data['High'] + new_data['Low'] + new_data['Close'])/4).cumsum()
new_data['VWAP'] = new_data['Cum_Vol_Price'] / new_data['Cum_Vol']
new_data = new_data.drop(['Cum_Vol','Cum_Vol_Price'],axis=1)

# Typecast 'Date' field to pandas datetime
new_data['Date'] = pd.to_datetime(new_data['Date'])

# View the data
new_data.head()

# Get Day of Week as feature
new_data['DOW'] = new_data['Date'].dt.dayofweek

# View the data
new_data.head()

# Get the Running Difference
new_data['Running Difference'] = new_data['High'] - new_data['Low'] 

# Get US/CAD prices 
import quandl
quandl.ApiConfig.api_key = 'GiZfJhoMD1_aeMWiatrA'
us_cad = quandl.get('FRED/DEXCAUS', start_date='2012-01-01').reset_index().rename(columns={'Value':'US/CAD'})


# Merge new_data with us_cad
data_to_write = new_data.merge(us_cad,on='Date')

# Get the final csv
data_to_write.to_csv(file,encoding='utf-8')