# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 13:59:45 2019

@author: syue003
"""

"""
Modeling customer churn for an e-commerce company with Python
How to use the Lifetimes package to predict non-contractual churn risk
It's more cost effective to retain existing customers than to acquire new ones, which is why it's important to track customers at high risk of turnover (churn) and target them with retention strategies.

In this project, I'll build a customer churn model based off of data from Olist, a Brazilian e-commerce site. I'll use that to identify high risk customers and inform retention strategies and marketing experiments.

There's one complication with e-commerce. While it's straightforward to measure churn for a contractual (subscription-based) business, churns aren't explicitly observed in non-contractual businesses (e-commerce). In these scenarios, probabilistic models come in handy for estimating time of customer death. The probabilistic model that I'll use is the BG/NBD model from the Lifetimes package.

Load the data
"""



import pandas as pd
import numpy as np
import datetime as dt
import seaborn as sns
import matplotlib.pyplot as plt

from lifetimes.utils import *
from lifetimes import BetaGeoFitter,GammaGammaFitter
from lifetimes.plotting import plot_probability_alive_matrix, plot_frequency_recency_matrix, plot_period_transactions, plot_cumulative_transactions,plot_incremental_transactions
from lifetimes.generate_data import beta_geometric_nbd_model
from lifetimes.plotting import plot_calibration_purchases_vs_holdout_purchases, plot_period_transactions,plot_history_alive

orders = pd.read_csv('C:\SYUE\RecSys\Data\olist_orders_dataset.csv')
items = pd.read_csv('C:\SYUE\RecSys\Data\olist_order_items_dataset.csv')
cust = pd.read_csv('C:\SYUE\RecSys\Data\olist_customers_dataset.csv')

print(cust.columns)
len(cust.groupby('customer_unique_id'))
cust.groupby('customer_unique_id').size().value_counts()

orders = pd.merge(orders,cust[['customer_id','customer_unique_id']],on='customer_id')
orders.head()
print(orders.columns)

items_bkup = items.copy()


print(items.columns)
items.shape
items.drop_duplicates('order_id',keep='first',inplace=True)
items.shape
"""
Join orders with items to append price information.
"""

transaction_data = pd.merge(orders,items,'inner','order_id')
print(transaction_data.columns)
transaction_data = transaction_data[['customer_unique_id','order_purchase_timestamp','price']]

transaction_data['date'] = pd.to_datetime(transaction_data['order_purchase_timestamp']).dt.date
transaction_data = transaction_data.drop('order_purchase_timestamp',axis=1)
transaction_data.head()
print(transaction_data.columns)


"""
Identify non unique bb
 customer_unique_id == 'b6c083700ca8c135ba9f0f132930d4e8' with two customer_id
 132  f7c5afab273b47ab517e096e0219b932  ...             SP
 679  c57b4b6f3719475543b721e720a526ad  ...             SP
"""
cust_dup =  cust[cust.customer_unique_id.duplicated()]
cust_dup.info()

cust [ cust .customer_unique_id == 'b6c083700ca8c135ba9f0f132930d4e8'].head()
order_1 = orders [ (orders.customer_id == 'f7c5afab273b47ab517e096e0219b932') | (orders.customer_id == 'c57b4b6f3719475543b721e720a526ad') ].head()
orders [ (orders.customer_id == 'f7c5afab273b47ab517e096e0219b932') | 
        (orders.customer_id == 'c57b4b6f3719475543b721e720a526ad') ].head()

from lifetimes.datasets import load_transaction_data 
from lifetimes.utils import summary_data_from_transaction_data

summary = summary_data_from_transaction_data(transaction_data,'customer_unique_id','date',monetary_value_col='price',)
summary.describe()
print(transaction_data.date.max())
print(transaction_data.date.min())