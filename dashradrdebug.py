import pandas as pd
import urllib
import json
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')

url_private = "https://dashradar.com/db/data/read-only_query?query=MATCH%20(d%3ADay)-%5B%3ALAST_BLOCK%5D-%3E(%3ABlock)-%5B%3APRIVATESEND_TOTALS%5D-%3E(bct%3APrivateSendTotals)%0ARETURN%20d.day*86400%20as%20time%2C%20bct.privatesend_tx_count%20as%20%60Number%20of%20PrivateSend%20transactions%60%0AORDER%20BY%20time%3B&params=%7B%7D"
response = urllib.urlopen(url_private)
data = json.loads(response.read())
private_transactions = pd.DataFrame(data["data"])
private_transactions['real'] = private_transactions[1].shift(-1)
private_transactions['diff'] = private_transactions['real'] - private_transactions[1]


private_transactions['yyyymmdd'] = pd.to_datetime(private_transactions[0],unit='s')
private_transactions['yyyymmdd'] = private_transactions.yyyymmdd.apply(lambda x: x.date().strftime('%Y-%m-%d'))
private_transactions['yyyymmdd'] = pd.to_datetime(private_transactions['yyyymmdd'])


stats_private_send = private_transactions[['yyyymmdd','diff']]


stats_private_send = stats_private_send.fillna(0)

print "done with Private Send........... \n\n"

print "inserting into DB ////// \n\n\n\n"
# insert in db
stats_private_send['yyyymmdd'] = pd.to_datetime(stats_private_send['yyyymmdd'])
stats_private_send = stats_private_send.set_index('yyyymmdd')
stats_private_send.to_sql('stats_private_tx', con=engine, if_exists='replace',index_label='yyyymmdd')
print "insert into stats_private_send \n"
print "done"