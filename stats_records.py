from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')
engine.execute('''
DROP TABLE IF EXISTS stats_records;
''')


engine.execute('''
CREATE TABLE stats_records AS 
SELECT 
days_from_ath,
close/high-1 as perc_down_ath,
record,
cur_value/record as perc_down_record,
j.value as circulating_supply 
FROM 
(SELECT DATE_PART('day', NOW()-yyyymmdd) as days_from_ath, high
FROM
  (SELECT yyyymmdd,
          max(close) as high
   FROM public.prices
   group by 1
   order by 2 DESC
   LIMIT 1)l)p
JOIN ( SELECT close FROM prices )y ON 1=1
JOIN (SELECT MAX(value) as record from statistic_tx_count_dash)o ON 1=1
JOIN (SELECT date, value as cur_value FROM statistic_tx_count_dash ORDER by 1 DESC)s ON 1=1
JOIN (SELECT SUM(value)/1e8 as value from statistic_reward_dash)j ON 1=1 
LIMIT 1;
''')
