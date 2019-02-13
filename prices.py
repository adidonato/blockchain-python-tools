
# coding: utf-8

# In[102]:


import pandas as pd
import json 
import time
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')


# In[21]:


today = time.strftime('%Y%m%d') 


# In[22]:


# today = "20181211"
url = "https://coinmarketcap.com/currencies/dash/historical-data/?start=20130428&end=" + today


# In[45]:


df = pd.read_html(url)


# In[46]:


df = pd.DataFrame(df[0])
prices = df


# In[47]:


prices['yyyymmdd'] = pd.to_datetime(prices['Date'])


# In[48]:


prices = prices.set_index('yyyymmdd')


# In[51]:


prices = prices.reset_index()


# In[95]:


prices = prices.rename(columns={'Market Cap':'marketcap','Open*': 'open', 'High': 'high', 'Low': 'low', 'Close**':'close', 'Volume':'volume'})


# In[55]:


del prices['Date']


# In[90]:


idx = pd.date_range('19-01-2014', '02-13-2014')
idx = pd.DataFrame(idx)
idx['yyyymmdd'] = idx[0]
del idx[0]
prices = prices.append(idx, sort=True)
prices = prices.fillna(method='ffill')


# In[110]:


prices = prices.set_index('yyyymmdd')


# In[114]:


# prices = prices.tail(1)


# In[115]:


# engine = create_engine('postgresql://postgres@45.77.238.158:5432/dashnode')
prices.to_sql('prices', con=engine, if_exists='replace',index_label='yyyymmdd')
# engine.execute('SELECT * FROM "prices" LIMIT 5').fetchall()

