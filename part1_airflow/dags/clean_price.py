import pendulum
from airflow.decorators import dag, task
from messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_price_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import MetaData, inspect, Table, Column, UniqueConstraint, Integer, Numeric, Float, Boolean
   
    @task()
    def create_table():
        metadata = MetaData()
        clean_flats_prices_table = Table(
            'clean_flats_prices',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer),
            Column('building_id', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Numeric),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            UniqueConstraint('flat_id', name='unique_employee_constraint_price_clean')
        )
        
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()
        if not inspect(conn).has_table(clean_flats_prices_table.name):
            metadata.create_all(conn)
        else:
            return 'Таблица уже создана!!!'

    @task()
    def extract():
        """
        #### Extract task
        """
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
            SELECT * FROM flats_prices
        """
        data = pd.read_sql(sql, conn).drop(columns=['id'])
        conn.close()
        return data
        

    @task()
    def transform(data: pd.DataFrame):
        """
        #### Transform task
        """
        #удаление дупликатов
        feature_cols = data.columns.drop('flat_id').tolist()
        is_duplicated_features = data.duplicated(subset=feature_cols, keep='first')
        data = data[~is_duplicated_features].reset_index(drop=True)

        #удаление пропусков
        num_cols = data.select_dtypes(['float', 'int']).drop(columns=['id', 'flat_id', 'building_id', 'building_type_int']) # теперь я ищу нули во всех колонках, где они могут появиться
        for col in num_cols:
            data[col].replace({0: None}, inplace=True)
        data.dropna()
        
        #отсев выбросов
        outliers_cols = ['kitchen_area', 'living_area', 'rooms', 'total_area', 'ceiling_height']
        threshold = 0.8
        potential_outliers = pd.DataFrame()

        for col in outliers_cols:
            Q1 = data[col].quantile(.20)
            Q3 = data[col].quantile(.80)
            IQR = Q3 - Q1
            margin = threshold*IQR
            lower = Q1 - margin
            upper = Q3 + margin
            potential_outliers[col] = ~data[col].between(lower, upper)

        outliers = potential_outliers.any(axis=1)
        data = data[~outliers].reset_index(drop=True)
        return data
        

    @task()
    def load(data: pd.DataFrame):
        """
        #### Load task
        """
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="clean_flats_prices",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_price_dataset()