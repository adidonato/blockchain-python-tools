


import pandas as pd
import json 
import time
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:dashnode@localhost:5432/dashnode')

print engine.execute('SELECT * FROM "prices" LIMIT 5').fetchall()

