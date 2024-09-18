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
def prepare_price_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import MetaData, inspect, Table, Column, UniqueConstraint, Integer, Numeric, Float, Boolean
   
    @task()
    def create_table():
        metadata = MetaData()
        flats_prices_table = Table(
            'flats_prices',
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
            UniqueConstraint('flat_id', name='unique_employee_constraint_price')
        )
        
        hook = PostgresHook('destination_db')
        conn = hook.get_sqlalchemy_engine()
        if not inspect(conn).has_table(flats_prices_table.name):
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
        select
            f.id as flat_id, f.building_id, f.floor, f.kitchen_area, f.living_area, f.rooms, f.is_apartment, f.studio, f.total_area, f.price,
            b.build_year, b.building_type_int, b.latitude, b.longitude, b.ceiling_height, b.flats_count, b.floors_total, b.has_elevator
        from flats as f
        left join buildings as b on b.id = f.building_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data
        

    @task()
    def transform(data: pd.DataFrame):
        """
        #### Transform task
        """
        return data
        

    @task()
    def load(data: pd.DataFrame):
        """
        #### Load task
        """
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_prices",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

prepare_price_dataset()