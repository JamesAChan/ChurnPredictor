# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 19:25:32 2017

@author: lkejji
"""
######SOURCES
#http://pythondata.com/working-large-csv-files-python/
#librairies
import time
import pandas as pd
import os
from sqlalchemy import create_engine
#create and populate the database
path= 'C:\\Users\\lkejji\\Downloads\\files_churnProject'

# your code

def database_from_csv(path):
    for filename in os.listdir(path):
        with open(os.path.join(path, filename)) as file:
            csv_database = create_engine('sqlite:///churnP.db')
            chunksize = 100000
            i = 0
            j = 1
            for df in pd.read_csv(file, chunksize=chunksize, iterator=True):
                  df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
                  df.index += j
                  i+=1
                  df.to_sql(filename, csv_database, if_exists='append')
                  j = df.index[-1] + 1

start_time = time.time()
database_from_csv(path)                  
elapsed_time = time.time() - start_time     
#15505.240909814835             
#
##Load data
#path= 'C:\\Users\\lkejji\\Downloads\\'
#members=pd.read_csv(os.path.join(path,'members.csv'))
#sample_submission_zero=pd.read_csv('C:\\Users\\lkejji\\Downloads\\sample_submission_zero.csv')
#train=pd.read_csv('C:\\Users\\lkejji\\Downloads\\train.csv')
#transactions=pd.read_csv('C:\\Users\\lkejji\\Downloads\\transactions.csv')
#user_logs=pd.read_csv('C:\\Users\\lkejji\\Downloads\\user_logs.csv')

#### Sickit learn


