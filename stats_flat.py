from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')
engine.execute('''
DROP TABLE IF EXISTS stats_flat;
''')


engine.execute('''
CREATE TABLE stats_flat 

  AS  SELECT 
            a.date AT TIME ZONE 'UTC' as yyyymmdd, 
            p.price as price,
            p.volume as exchange_vol,
            p.marketcap as marketcap,
            volatility as volatility,
            a.value as addresses,
            b.value as avg_difficulty,
            c.value as block_count,
            e.value as block_size,
            f.value as fees_dash,
           (f.value/1e8)*price as fees_dash_usd,
            h.value as median_fee_dash,
            h.value*price as median_fee_usd,
            i.value as median_tx_value_dash,
            i.value*price as median_tx_value_usd,
            j.value as payment_count,
            k.value as rewards,
            l.value as tx_count,
            m.value as tx_volume_dash,
            m.value*price as tx_volume_usd,
            g.hashrate as hashrate,
            w.ios,
            w.android,
            w.desktop,
           COALESCE(SUM(ix),0) as instant_tx,
           COALESCE(SUM(mx),0) as mix_tx,
            COALESCE(SUM(px),0) as private_tx
--            SUM(w.ios+w.android+w.desktop) as wallet_downloads


      FROM 
            (
              SELECT 
                    yyyymmdd, 
                    SUM(close+open)/2 as price,
                    sum(marketcap) as marketcap, 
                    SUM(volume) as volume
                    FROM prices 
                    GROUP BY 1
            )p 

      INNER JOIN  
            daily_volatility_90 d 
            ON p.yyyymmdd=d.yyyymmdd
      INNER JOIN  
            statistic_active_addresses_dash a 
            ON p.yyyymmdd=a.date
      INNER JOIN  
            statistic_average_difficulty_dash b
            ON p.yyyymmdd=b.date
      INNER JOIN 
            statistic_block_count_dash c 
            ON p.yyyymmdd=c.date
      INNER JOIN
            statistic_block_size_dash e
            ON p.yyyymmdd=e.date
      INNER JOIN
            statistic_fees_dash f 
            ON p.yyyymmdd=f.date
      LEFT OUTER JOIN
            (SELECT SUM(value) as hashrate, yyyymmdd FROM statistic_hashrate_dash GROUP BY 2)g 
            ON p.yyyymmdd=g.yyyymmdd
      INNER JOIN
            statistic_median_fee_dash h 
            ON p.yyyymmdd=h.date
      INNER JOIN
            statistic_median_tx_value_dash i 
            ON p.yyyymmdd=i.date
      INNER JOIN
            statistic_payment_count_dash j 
            ON p.yyyymmdd=j.date
      INNER JOIN
            statistic_reward_dash k 
            ON p.yyyymmdd=k.date
      INNER JOIN
            statistic_tx_count_dash l 
            ON p.yyyymmdd=l.date
      INNER JOIN
            statistic_tx_volume_dash m 
            ON p.yyyymmdd=m.date
      LEFT OUTER JOIN
            stats_wallets w 
            ON p.yyyymmdd=w.yyyymmdd
      INNER JOIN
           (SELECT ppx.yyyymmdd as yyyymmdd, 
           SUM(ppx.value) as px, 
           SUM(mx.value) as mx, 
           SUM(ix.value)  as ix 
           FROM stats_private_tx ppx 
      LEFT OUTER JOIN
            stats_instant_tx ix 
            ON ppx.yyyymmdd=ix.yyyymmdd
      LEFT OUTER JOIN
      stats_mixed_tx mx
           ON ppx.yyyymmdd=mx.yyyymmdd
           GROUP BY 1)px
           ON px.yyyymmdd=p.yyyymmdd
      GROUP BY 
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16, 17, 18,19,20,21,22,23,24
      ORDER BY 1 DESC;
''')
