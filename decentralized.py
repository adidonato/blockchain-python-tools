# 
# script gets extraction info from arewedecentralizedyet.com and inserts it into table in postgres for reporting 
# @adidonato
# 
import pandas as pd
import json 
# import time
import urllib
import requests
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')
url = "https://arewedecentralizedyet.com/api.json"
header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}

r = requests.get(url, headers=header)

dfs = pd.read_json(r.text)
dfs = dfs.transpose()

dfs['yyyymmdd'] = pd.to_datetime('today')
dfs['yyyymmdd'] = dfs.yyyymmdd.apply(lambda x: x.date().strftime('%Y-%m-%d'))
dfs = dfs.set_index(['yyyymmdd','symbol'])

dfs.to_sql('stats_decentralized', con=engine, if_exists='append',index=True, index_label=['yyyymmdd','symbol'])
