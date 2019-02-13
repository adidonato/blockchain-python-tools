import pandas as pd
import json 
import time
from sqlalchemy import create_engine
# engine = create_engine('postgresql://postgres:dashnode@68.183.76.132:5432/dashnode')
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')



w = pd.read_csv("/root/wallet-downloads.csv")
w['yyyymmdd'] = pd.to_datetime(w['yyyymmdd'])
w = w.set_index('yyyymmdd')
w.to_sql('stats_wallets', con=engine, if_exists='replace')
print w.columns

print engine.execute('SELECT * FROM "stats_wallets" LIMIT 5').fetchall()
