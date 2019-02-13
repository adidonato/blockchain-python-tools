from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')
engine.execute('''
DROP TABLE IF EXISTS daily_volatility_90;
''')


engine.execute('''
CREATE TABLE daily_volatility_90 AS
SELECT yyyymmdd, close, day_before, diff,
stddev_pop(diff)
OVER (ORDER BY yyyymmdd ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) as volatility
FROM
(SELECT *, LAG(close, 1) OVER (ORDER BY yyyymmdd) as day_before,  close/(LAG(close, 1) OVER (ORDER BY yyyymmdd))-1 as diff
  FROM prices)p;
''')
