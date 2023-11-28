import os

import pandas as pd
from sqlalchemy import create_engine, text


class DbHelper():
    host = os.environ.get('DB_HOST')
    password = os.environ.get('DB_PASSWORD')
    engine = None

    def __init__(self):
        try:
            self.engine = create_engine(f"mysql+mysqlconnector://admin:{self.password}@{self.host}:3306/marketData")
        except Exception as e:
            print(f'--> error is: {e}')
            raise e

    def store_df(self, df: pd.DataFrame):
        try:
            store_resp = df.to_sql('stocks', self.engine, if_exists='append')
            return store_resp
        except Exception as e:
            print(f'--> error is: {e}')
            raise e

    def execute_query(self, statement: str):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(statement))
                return result
        except Exception as e:
            print(f'--> error is: {e}')
            raise e

    def execute_update(self, statement: str):
        try:
            with self.engine.connect() as connection:
                connection.execute(text(statement))
                connection.commit()
        except Exception as e:
            print(f'--> error is: {e}')
            raise e
