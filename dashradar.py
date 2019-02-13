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

stats_private_send = stats_private_send.rename(columns={'diff':'value'})
print "done with Private Send........... \n\n"

stats_private_send['yyyymmdd'] = pd.to_datetime(stats_private_send['yyyymmdd'])
stats_private_send = stats_private_send.set_index('yyyymmdd')
stats_private_send.to_sql('stats_private_tx', con=engine, if_exists='replace',index_label='yyyymmdd')
print "insert into stats_private_send \n"
# mixing
url_mix = "https://dashradar.com/db/data/read-only_query?query=MATCH%20(d%3ADay)-%5B%3ALAST_BLOCK%5D-%3E(%3ABlock)-%5B%3APRIVATESEND_TOTALS%5D-%3E(pst%3APrivateSendTotals)%0ARETURN%20d.day*86400%20as%20time%2C%20pst.privatesend_mixing_0_01_count%2Bpst.privatesend_mixing_0_1_count%2Bpst.privatesend_mixing_1_0_count%2Bpst.privatesend_mixing_10_0_count%20as%20%60Number%20of%20mixing%20transactions%60%0AORDER%20BY%20time%3B&params=%7B%7D"
response = urllib.urlopen(url_mix)
data = json.loads(response.read())
mix_transactions = pd.DataFrame(data["data"])
mix_transactions['real'] = mix_transactions[1].shift(-1)
mix_transactions['diff'] = mix_transactions['real'] - mix_transactions[1]
mix_transactions['yyyymmdd'] = pd.to_datetime(mix_transactions[0],unit='s')
mix_transactions['yyyymmdd'] = mix_transactions.yyyymmdd.apply(lambda x: x.date().strftime('%Y-%m-%d'))
mix_transactions['yyyymmdd'] = pd.to_datetime(mix_transactions['yyyymmdd'])
mix_transactions = mix_transactions[['yyyymmdd','diff']]
mix_transactions = mix_transactions.rename(columns={'diff':'value'})
print "done with Mixing Tx........ \n\n\n\n"

mix_transactions['yyyymmdd'] = pd.to_datetime(mix_transactions['yyyymmdd'])
mix_transactions = mix_transactions.set_index('yyyymmdd')
mix_transactions.to_sql('stats_mixed_tx', con=engine, if_exists='replace',index_label='yyyymmdd')
print "insert into mixes \n\n\n"




url_insta = "https://dashradar.com/db/data/read-only_query?query=MATCH%20(tx%3ATransaction)-%5B%3AINCLUDED_IN%5D-%3E(b%3ABlock)%20WHERE%20b.time%20%3E%201530396000%20and%20b.time%20%3C%201535752800%20AND%20tx.txlock%3Dtrue%20RETURN%20date(datetime(%7BepochSeconds%3Ab.time%7D))%20as%20date%2C%20count(tx)%20as%20ixcount%20ORDER%20BY%20date%3B&params=%7B%7D"
insta_response = urllib.urlopen(url_insta)
insta_json = json.load(insta_response)
insta_frame = pd.DataFrame(insta_json['data'])
instant = pd.DataFrame(columns=[0,1])
first = insta_frame
instant = instant.append(first)
url_insta = "https://dashradar.com/db/data/read-only_query?query=MATCH%20(tx%3ATransaction)-%5B%3AINCLUDED_IN%5D-%3E(b%3ABlock)%20WHERE%20b.time%20%3E%201535752800%20and%20b.time%20%3C%201541030400%20AND%20tx.txlock%3Dtrue%20RETURN%20date(datetime(%7BepochSeconds%3Ab.time%7D))%20as%20date%2C%20count(tx)%20as%20ixcount%20ORDER%20BY%20date%3B&params=%7B%7D"
insta_response = urllib.urlopen(url_insta)
insta_json = json.load(insta_response)
insta_frame = pd.DataFrame(insta_json['data'])
second = insta_frame
instant = instant.append(second)
url_insta = "https://dashradar.com/db/data/read-only_query?query=MATCH%20(tx%3ATransaction)-%5B%3AINCLUDED_IN%5D-%3E(b%3ABlock)%20WHERE%20b.time%20%3E%201541030401%20and%20b.time%20%3C%201546214400%20AND%20tx.txlock%3Dtrue%20RETURN%20date(datetime(%7BepochSeconds%3Ab.time%7D))%20as%20date%2C%20count(tx)%20as%20ixcount%20ORDER%20BY%20date%3B&params=%7B%7D"
insta_response = urllib.urlopen(url_insta)
insta_json = json.load(insta_response)
insta_frame = pd.DataFrame(insta_json['data'])
third = insta_frame
instant = instant.append(third)
url_insta = "https://dashradar.com/db/data/read-only_query?query=MATCH%20(tx%3ATransaction)-%5B%3AINCLUDED_IN%5D-%3E(b%3ABlock)%20WHERE%20b.time%20%3E%201546214401%20and%20b.time%20%3C%209999999999%20AND%20tx.txlock%3Dtrue%20RETURN%20date(datetime(%7BepochSeconds%3Ab.time%7D))%20as%20date%2C%20count(tx)%20as%20ixcount%20ORDER%20BY%20date%3B&params=%7B%7D"
insta_response = urllib.urlopen(url_insta)
insta_json = json.load(insta_response)
insta_frame = pd.DataFrame(insta_json['data'])
fourth = insta_frame
instant = instant.append(fourth)
instant=instant.groupby(0).sum()

instant = instant.reset_index()

instant = instant.rename(columns={1:'value',0:'yyyymmdd'})

print "done Instant Send ............\n\n\n"
print "inserting into DB ////// \n\n\n\n"
# insert in db
instant['yyyymmdd'] = pd.to_datetime(instant['yyyymmdd'])
instant = instant.set_index('yyyymmdd')
instant.to_sql('stats_instant_tx', con=engine, if_exists='replace',index_label='yyyymmdd')
print "done"