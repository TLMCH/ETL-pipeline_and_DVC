import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import yaml


def create_connection():

    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    
    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', connect_args={'sslmode':'require'})
    return conn


def get_data():
    conn = create_connection()
    data = pd.read_sql('select * from clean_flats_prices', conn)
    conn.dispose()


    os.makedirs('data', exist_ok=True)
    data.to_csv('data/initial_data.csv', index=None)


if __name__ == '__main__':
    get_data() 