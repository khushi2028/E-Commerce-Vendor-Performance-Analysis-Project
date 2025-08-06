import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)

engine=create_engine("sqlite:///inventory.db")

def db_ingest(df,table_name,engine):
    '''this function will ingest dataframe into database table'''
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)

def load_raw_data():
    '''this function will load CSVs as dataframes and ingest into db'''
    start=time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df=pd.read_csv('data/'+ file)
            logging.info(f'Ingesting {file} in db')
            db_ingest(df,file[:-4],engine)
    end=time.time()
    total_time_taken=(end-start)/60
    logging.info("------INGESTION COMPLETE--------")
    logging.info(f'Total time taken for indestion is {total_time_taken} minutes')

if __name__=="__main__":
    load_raw_data()