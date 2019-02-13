import pandas as pd
import time 
from datetime import datetime 
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')

ScriptStartTime = datetime.now()
engine.execute('''
CREATE TABLE IF NOT EXISTS addresses_balance_usd3 (yyyymmdd DATE, address VARCHAR, balance DECIMAL, balance_usd DECIMAL)
''')
print "\n\n CREATED TABLE \n\n"

m = pd.date_range(start='12/1/2013', end='12/1/2018', closed='right', freq='MS')
for single_date in m:
    startTime = datetime.now()
    day_from = (single_date.strftime("%Y-%m-%d"))
    day_until = single_date + relativedelta(months=1)  
    day_until = (day_until.strftime("%Y-%m-%d"))
    query = ('''
    
    INSERT INTO addresses_balance_usd3
    WITH myconstants (day_from, day_until) as (
       values ('%s'::date, '%s'::date)
    ),
        massive as (WITH
    rogue_join as ( WITH inp as (select yyyymmdd::date, address, input as neg, cum_in
      FROM inputs_addresses, myconstants
      WHERE yyyymmdd >= day_from AND yyyymmdd < day_until
    ORDER BY yyyymmdd)
    SELECT p.yyyymmdd, inp.address, close FROM prices p, myconstants
    CROSS JOIN inp
    WHERE p.yyyymmdd >= day_from AND p.yyyymmdd < day_until  
    GROUP BY 2,1, 3 ORDER BY 1,2),
    inp as (select yyyymmdd::date, address, input as neg, cum_in
      FROM inputs_addresses, myconstants
      WHERE yyyymmdd >= day_from AND yyyymmdd < day_until
    ORDER BY yyyymmdd),
    outp as (select yyyymmdd::date, address, output*(-1) as neg, cum_out
    FROM outputs_addresses, myconstants
    WHERE yyyymmdd >= day_from AND yyyymmdd < day_until
    ORDER BY yyyymmdd)
    
    SELECT r.yyyymmdd, r.address, close, cum_in, cum_out,
    COALESCE(locf(cum_in) OVER (PARTITION BY r.address ORDER BY r.address, r.yyyymmdd),0) as ffill,
    COALESCE(locf(cum_out) OVER (PARTITION BY r.address ORDER BY r.address, r.yyyymmdd),0) as ffill_out
    FROM rogue_join r
    LEFT JOIN inp  ON (r.yyyymmdd=inp.yyyymmdd AND r.address=inp.address)
    LEFT JOIN outp ON (r.yyyymmdd=outp.yyyymmdd AND r.address=outp.address) ORDER BY 2,1)
    SELECT yyyymmdd,address, ffill-ffill_out as balance, (ffill-ffill_out)*close as balance_usd FROM massive
    GROUP BY 1,2, ffill, ffill_out, close ORDER BY 1,2
    -- HAVING ffill-ffill_out > 0
    ;
    ''' % (day_from, day_until))
    #print query 
    print "Sleeping for 10 seconds.....\n\n\n"
    time.sleep(10)
    print "CURRENTLY PROCESSING\n\n"
    print query 
    print "\n\n"
    #print "!!!!!!!!!!!!!!!! DRY RUN !!!!!!!!!!!!!!!!!!!!\n\n\n\n" 
    engine.execute(query)
    print "DONE PROCESSING ",day_from," UNTIL ",day_until, "\n\n"
    print "Processed ", day_from, "in ", datetime.now() - startTime, " seconds.... On to the next one...\n\n" 


print "\n\n\n\n\n\n !!!!!!!!!!!!!!! DONE PROCESSING ADDRESSES !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" 
print "TIME TAKEN: ", datetime.now() - ScriptStartTime 
